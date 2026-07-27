import os

from behave import step, then


def _launcher_log(context, name):
    return os.path.join(context.pctl.output_dir, 'exec_prefix_{0}.log'.format(name))


@step('I configure and start {name:name} with a postgres exec prefix')
def start_patroni_with_exec_prefix(context, name):
    launcher = os.path.join(context.pctl.patroni_path, 'features', 'exec_prefix_launcher.py')
    return context.pctl.start(name, custom_config={'postgresql': {'postgres_exec_prefix': [
        context.pctl.PYTHON, launcher.replace('\\', '/'), '--log',
        _launcher_log(context, name).replace('\\', '/'), '--']}})


@then('the exec prefix of {name:name} recorded {count:d} or more postgres executions')
def check_recorded_executions(context, name, count):
    logfile = _launcher_log(context, name)
    lines = []
    if os.path.exists(logfile):
        with open(logfile) as f:
            lines = [line.strip() for line in f if line.strip()]
    assert len(lines) >= count, \
        'the exec prefix of {0} recorded {1} executions, expected at least {2}'.format(name, len(lines), count)
    for line in lines:
        argv = line.split('\t')
        assert os.path.basename(argv[0]) in ('postgres', 'postgres.exe'), \
            'the exec prefix of {0} was asked to execute {1} instead of postgres'.format(name, argv[0])
