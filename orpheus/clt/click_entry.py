import os
import yaml
import click

from orpheus.core.db import DatabaseConnection
from orpheus.core.exception import BadStateError, NotImplementedError, BadParametersError
from orpheus.core.executor import Executor
from orpheus.core.sql_parser import SQLParser
from orpheus.core.user_control import UserManager, InvalidCredentialError

class Context():
    config_file = 'config.yaml'

    def __init__(self):
        if 'ORPHEUS_HOME' not in os.environ:
            os.environ['ORPHEUS_HOME'] = os.getcwd()
        self.config_path = os.environ['ORPHEUS_HOME'] + '/' + self.config_file
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.load(f)

            assert(self.config['orpheus']['home'] != None)

            if not self.config['orpheus']['home'].endswith('/'):
                self.config['orpheus']['home'] += '/'
            # if user overwrites ORPHEUS_HOME, rewrite environmental parameters
            try:
                os.environ['ORPHEUS_HOME'] = self.config['orpheus']['home']
            except KeyError:
                pass
        except (IOError, KeyError) as e:
            raise BadStateError("%s file not found or data not clean, abort" % self.config_file)
        except AssertionError as e:
            raise BadStateError("orpheus_home not specified in %s" % config.yaml)
        except: # unknown error
            raise BadStateError("unknown error while loading config file, abort")

@click.group()
@click.pass_context
def cli(ctx):
    try:
        ctx.obj = Context().config
        user_obj = UserManager.get_current_state()
        for key in user_obj:
            ctx.obj[key] = user_obj[key]
    except Exception as e:
        click.secho(str(e), fg='red')

@cli.command()
@click.option('--database', prompt='Enter database name', help='Specify the database name that you want to configure to.')
@click.option('--user', prompt='Enter user name', help='Specify the user name that you want to configure to.')
@click.option('--password', prompt=True, hide_input=True, help='Specify the password.', default='')
@click.pass_context
def config(ctx, user, password, database):
    newctx = ctx.obj

    try:
        newctx['database'] = database
        newctx['user'] = user
        newctx['passphrase'] = password
        conn = DatabaseConnection(newctx)
    except Exception as e:
        click.secho(str(e), fg='red')
        return

    try:
        UserManager.create_user(user, password)
        if UserManager.verify_credential(user, password):
            UserManager.create_user(user, password)
            from orpheus.core.encryption import EncryptionTool
            newctx['passphrase'] = EncryptionTool.passphrase_hash(password)
            click.echo('Logged into the database [%s] as [%s]' % (database, user))
    except InvalidCredentialError as e:
        click.secho('Invalid credentials for [%s]' % user)
    except Exception as e:
        click.secho(str(e), fg='red')

    click.echo(ctx.obj)

@cli.command()
@click.pass_context
def create_user(ctx):
    click.echo(ctx.obj)
    # check if this user has permission to create new user
    # create user in UserManager
    if not ctx.obj['user'] or not ctx.obj['database']:
        click.secho("No session in use, please call config first", fg='red')
        return # skip the following commands

    user = click.prompt('Please enter user name')
    password = click.prompt('Please enter password', hide_input=True, confirmation_prompt=True)

    click.echo("Creating user [%s] for database [%s]" % (ctx.obj['user'], ctx.obj['database']))
    try:
        conn = DatabaseConnection(ctx.obj)
        UserManager.create_user(conn, user, password)
        click.echo('User created.')
    except Exception as e:
        click.secho(str(e), fg='red')

    # TODO: check permission?

@cli.command()
@click.pass_context
def whoami(ctx):
    if not ctx.obj['user'] or not ctx.obj['database']:
        click.secho("No session is in use, please call config first", fg='red')
    else:
        click.echo("Logged into the database [%s] as [%s]" % (ctx.obj['database'], ctx.obj['user']))

@cli.command()
@click.argument('input_file', type=click.Path(exists=True))
@click.argument('dataset')
@click.option('--table_name', '-t', help='Create the dataset with existing table schema')
@click.option('--schema', '-s', help='Create the dataset with schema file', type=click.Path(exists=True))
@click.pass_context
def init(ctx, input_file, dataset, table_name, schema):
    # TODO: add header support
    # By default, we connect to the database specified in the -config- command earlier

    # Two cases need to be taken care of:
    # 1.add version control on an outside file
    #    1.1 Load a csv or other format of the file into DB
    #    1.2 Schema
    # 2.add version control on a existing table in DB
    conn = DatabaseConnection(ctx.obj)
    executor = Executor(ctx.obj, conn)
    executor.exec_init(input_file, dataset, table_name, schema, conn)


@cli.command()
@click.argument('dataset')
@click.pass_context
def drop(ctx, dataset):
    if click.confirm("Are you sure you want to drop %s?" % dataset):
        try:
            conn = DatabaseConnection(ctx.obj)
            click.echo("Dropping dataset [%s] ..." % dataset)
            executor = Executor(ctx.obj, conn)
            executor.exec_drop(dataset)
        except Exception as e:
            click.secho(str(e), fg='red')

@cli.command()
@click.option('--dataset', '-d', help='Specify the dataset to show')
@click.option('--table_name', '-t', help='Specify the table to show')
@click.pass_context
def ls(ctx, dataset, table_name):
    # if no dataset specified, show the list of dataset the current user owns
    try:
        conn = DatabaseConnection(ctx.obj)
        click.echo("The current database contains the following CVDs:")
        if not dataset:
            click.echo("\n".join(conn.list_dataset()))
        else:
            click.echo(conn.show_dataset(dataset))
            # when showing dataset, remove rid
    except Exception as e:
        click.secho(str(e), fg='red')

# the callback function to execute file
# execute line by line
def execute_sql_file(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    # value is the relative path of file
    conn = DatabaseConnection(ctx.obj)
    parser = SQLParser(conn)
    abs_path = ctx.obj['orpheus']['home'] + value
    click.echo("Executing SQL file at %s" % value)
    with open(abs_path, 'r') as f:
        for line in f:
            executable_sql = parser.parse(line)
    ctx.exit()

@cli.command()
@click.option('--file', '-f', callback=execute_sql_file, expose_value=False, is_eager=True, type=click.Path(exists=True))
@click.option('--sql', prompt="Input sql statement")
@click.pass_context
def run(ctx, sql):
    # TODO: add finer grained try-catch for SQLParser
    try:
        # execute_sql_line(ctx, sql)
        conn = DatabaseConnection(ctx.obj)
        parser = SQLParser(conn)
        executable_sql = parser.parse(sql)
        # print executable_sql
        conn.execute_sql(executable_sql)

    except Exception as e:
        import traceback
        traceback.print_exc()
        click.secho(str(e), fg='red')

@cli.command()
@click.argument('dataset')
@click.option('--vlist', '-v', multiple=True, required=True, help='Specify version you want to checkout, use multiple -v for multiple version checkout')
@click.option('--to_table', '-t', help='Specify the table name to checkout to.')
@click.option('--to_file', '-f', help='Specify the location of file')
@click.option('--delimiters', '-d', default=',', help='Specify the delimiter used for checkout file')
@click.option('--header', '-h', is_flag=True, help="If set, the first line of checkout file will be the header")
@click.option('--ignore/--no-ignore', default=False, help='If set, checkout versions into table will ignore duplicated key')
@click.pass_context
def checkout(ctx, dataset, vlist, to_table, to_file, delimiters, header, ignore):
    conn = DatabaseConnection(ctx.obj)
    executor = Executor(ctx.obj, conn)
    executor.exec_checkout(dataset, vlist, to_table, to_file, delimiters, header, ignore, conn)


@cli.command()
@click.option('--msg','-m', help='Commit message', required=True)
@click.option('--table_name','-t', help='The table to be committed') # changed to optional later
@click.option('--file_name', '-f', help='The file to be committed', type=click.Path(exists=True))
@click.option('--delimiters', '-d', default=',', help='Specify the delimiters used for checkout file')
@click.option('--header', '-h', is_flag=True, help="If set, the first line of checkout file will be the header")
@click.pass_context
def commit(ctx, msg, table_name, file_name, delimiters, header):
    conn = DatabaseManager(ctx.obj)
    executor = Executor(ctx.obj)
    executor.exec_commit(msg, table_name, file_name, delimiters, header)

@cli.command()
@click.pass_context
def clean(ctx):
    config = ctx.obj
    open(config['meta']['info'], 'w').close()
    f = open(config['meta']['info'], 'w')
    f.write('{"file_map": {}, "table_map": {}, "table_created_time": {}, "merged_tables": []}')
    f.close()
    click.echo("meta.info cleaned")
    open(config['meta']['modifiedIds'], 'w').close()
    f = open(config['meta']['modifiedIds'], 'w')
    f.write('{}')
    f.close()
    click.echo("meta.modifiedID cleaned")

@cli.command()
@click.option('--dataset', '-d', help='CVD name', required=True)
@click.pass_context
def status(ctx, dataset):
    # TODO: show checked out versions and their schemas, etc.
    click.secho("orpheus status not yet implemented", fg='red')