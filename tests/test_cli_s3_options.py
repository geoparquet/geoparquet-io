"""Tests for global S3 CLI options."""

import importlib

from click.testing import CliRunner

main_module = importlib.import_module("geoparquet_io.cli.main")
cli = main_module.cli


class TestHiddenAliases:
    """Test that per-command S3 flags still work when hidden."""

    def test_upload_hidden_s3_endpoint_still_works(self):
        """Per-command --s3-endpoint on publish upload still accepted."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["publish", "upload", "--s3-endpoint", "minio.local:9000", "nonexistent", "s3://b/f"],
        )
        assert "No such option" not in result.output

    def test_hidden_aws_profile_on_convert_still_works(self):
        """Per-command --aws-profile on convert reproject still accepted."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["convert", "reproject", "--aws-profile", "prod", "nonexistent.parquet"],
        )
        assert "No such option" not in result.output


class TestGlobalToCommandWiring:
    """Test that global flags feed into commands via ambient config."""

    def test_global_s3_endpoint_does_not_error(self):
        """Global --s3-endpoint is accepted without 'unknown option' on subcommands."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["--s3-endpoint", "data.source.coop", "inspect", "summary", "nonexistent.parquet"],
        )
        assert "No such option" not in result.output


class TestGroupInvokedDirectly:
    """A group object invoked on its own must not need its callback to have
    primed ``ctx.obj``.

    The root ``cli`` group calls ``ctx.ensure_object(dict)``, so every real CLI
    path arrives at ``_activate_s3`` with a dict. Invoking a group object
    directly -- what tests and programmatic callers do -- skips the root, and
    ``_activate_s3`` then hit ``None.get(...)``: ``process`` was the group with
    no ``ensure_object`` of its own and four ``_activate_s3`` call sites
    (#922 item 1). Making the helper enforce its own precondition covers every
    group, present and future, rather than the ten callbacks each remembering.
    """

    def test_activate_s3_primes_an_unset_ctx_obj(self):
        """The helper itself tolerates ``ctx.obj is None``."""
        import click

        from geoparquet_io.cli._shared import _activate_s3

        ctx = click.Context(click.Command("standalone"))
        assert ctx.obj is None

        with _activate_s3(ctx):
            pass

        assert ctx.obj == {}

    def test_process_group_invoked_directly(self, tmp_path):
        """``process`` has no ``ensure_object`` of its own -- the regression."""
        from geoparquet_io.cli.commands.process import process

        result = CliRunner().invoke(
            process,
            ["aggregate", "h3", str(tmp_path / "missing.parquet"), str(tmp_path / "out.parquet")],
        )
        assert not isinstance(result.exception, AttributeError), result.exception

    def test_add_group_invoked_directly(self, buildings_test_file, tmp_path):
        """A group that does carry ``ensure_object`` keeps working unchanged."""
        from geoparquet_io.cli.commands.add import add

        result = CliRunner().invoke(
            add,
            ["bbox", buildings_test_file, str(tmp_path / "out.parquet")],
        )
        assert not isinstance(result.exception, AttributeError), result.exception
        assert result.exit_code == 0, result.output

    def test_benchmark_group_invoked_directly(self):
        """``benchmark``'s callback body is only ``ensure_object`` -- removing it
        must not change how the group behaves on its own."""
        from geoparquet_io.cli.commands.benchmark import benchmark

        result = CliRunner().invoke(benchmark, ["--help"])
        assert result.exit_code == 0, result.output
        assert "suite" in result.output
