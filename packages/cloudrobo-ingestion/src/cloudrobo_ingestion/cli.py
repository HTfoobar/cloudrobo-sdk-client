"""CLI plugin; runtime imports are deferred until a command is invoked."""

import json

import click

from .config import RUN_MODES


@click.group()
def ingestion():
    """机器人本地数据回流到 OBS。"""


@ingestion.command("start")
@click.option(
    "--config", "config_path", required=True, type=click.Path(exists=True, dir_okay=False)
)
@click.option("--run-mode", type=click.Choice(RUN_MODES), default=None)
@click.option("--source-dir", type=click.Path(file_okay=False), default=None)
@click.option("--work-dir", type=click.Path(file_okay=False), default=None)
@click.option("--obs-path", default=None)
@click.option("--task-type", default=None)
@click.option("--collect-method", default=None)
@click.option("--max-files-per-batch", type=click.IntRange(min=1), default=None)
@click.option("--validate-lerobot/--no-validate-lerobot", "validation_enabled", default=None)
@click.option("--zenoh-mode", type=click.Choice(["client", "peer"]), default=None)
@click.option("--zenoh-endpoint", multiple=True)
@click.option("--zenoh-project-id", default=None)
@click.option("--zenoh-device-id", default=None)
@click.option("--zenoh-client-id", default=None)
@click.option("--mock-file-count", type=int, default=None)
@click.option("--mock-file-size-bytes", type=int, default=None)
@click.option("--mock-upload-bps", type=float, default=None)
@click.option("--mock-batch-interval-seconds", type=float, default=None)
def start(config_path, **overrides):
    """启动后台目录监控。"""
    from . import lifecycle

    try:
        result = lifecycle.start(config_path, overrides=overrides)
    except (OSError, ValueError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        f"回流进程已启动，模式={result['run_mode']}，PID={result['pid']}，工作目录={result['work_dir']}"
    )


@ingestion.command("status")
@click.option("--json", "as_json", is_flag=True, help="输出 JSON 状态快照")
def status(as_json):
    """离线查询进程及最近的回流状态。"""
    from . import lifecycle

    result = lifecycle.status()
    if as_json:
        click.echo(json.dumps(result, ensure_ascii=False))
        return
    click.echo(f"进程: {'运行中' if result['running'] else '已停止'}")
    click.echo(f"运行模式: {result.get('run_mode', 'upload_and_report')}")
    click.echo(f"批次统计: {result.get('counts', {})}")
    click.echo(f"当前批次: {result.get('active_batch') or '-'}")
    click.echo(f"上传进度: {result.get('progress', {})}")
    reporter = result.get("reporter", {})
    click.echo(f"上报状态: {reporter if reporter.get('enabled', True) else '未启用'}")
    for item in result.get("batch_errors", []):
        click.echo(f"{item['path']}: {item['error']}")
    if result.get("error"):
        click.echo(f"最近错误: {result['error']}")


@ingestion.command("stop")
def stop():
    """尽快结束当前上传并退出后台进程。"""
    from . import lifecycle

    try:
        lifecycle.stop()
    except (OSError, ValueError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo("回流进程已停止")
