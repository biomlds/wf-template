"""Create backup workflow report."""
import json
import os

from dominate.tags import b, p, div, pre
from ezcharts.components.reports import labs
from ezcharts.layout.snippets.table import DataTable
import pandas as pd

from .util import get_named_logger, wf_parser


def main(args):
    """Run the entry point."""
    logger = get_named_logger("Report")
    logger.info("Creating backup workflow report.")

    report_title = "Backup Workflow Report"
    report = labs.LabsReport(
        report_title, "wf-backup",
        args.params, args.versions, args.wf_version)

    with report.add_section("Backup Summary", "Summary"):
        p("This report documents the backup operation performed by the wf-backup workflow.")

        ont_data = None
        epi2me_data = None

        if args.ont_manifest and os.path.exists(args.ont_manifest) and os.path.getsize(args.ont_manifest) > 0:
            with open(args.ont_manifest) as f:
                ont_data = json.load(f)
            p(b("ONT Data Backup:"))
            p(f"Total files backed up: {ont_data.get('total_files', 0)}")
            p("")

        if args.epi2me_manifest and os.path.exists(args.epi2me_manifest) and os.path.getsize(args.epi2me_manifest) > 0:
            with open(args.epi2me_manifest) as f:
                epi2me_data = json.load(f)
            p(b("EPI2ME Data Backup:"))
            p(f"Total files backed up: {epi2me_data.get('total_files', 0)}")
            p("")

    if args.ont_manifest and os.path.exists(args.ont_manifest) and os.path.getsize(args.ont_manifest) > 0 and ont_data:
        with report.add_section("ONT Data Files", "ONT Files"):
            if ont_data.get('files'):
                df = pd.DataFrame(ont_data['files'])
                df['filename'] = df['path'].apply(os.path.basename)
                df = df[['filename', 'checksum', 'path']]
                df = df.rename(columns={'checksum': 'MD5 Checksum', 'path': 'Full Path', 'filename': 'Filename'})
                DataTable.from_pandas(df)
            else:
                p("No files were backed up.")

    if args.epi2me_manifest and os.path.exists(args.epi2me_manifest) and os.path.getsize(args.epi2me_manifest) > 0 and epi2me_data:
        with report.add_section("EPI2ME Data Files", "EPI2ME Files"):
            if epi2me_data.get('files'):
                df = pd.DataFrame(epi2me_data['files'])
                df['filename'] = df['path'].apply(os.path.basename)
                df = df[['filename', 'checksum', 'path']]
                df = df.rename(columns={'checksum': 'MD5 Checksum', 'path': 'Full Path', 'filename': 'Filename'})
                DataTable.from_pandas(df)
            else:
                p("No files were backed up.")

    if args.ont_log and os.path.exists(args.ont_log) and os.path.getsize(args.ont_log) > 0:
        with report.add_section("ONT Backup Log", "ONT Log"):
            with open(args.ont_log) as f:
                log_content = f.read()
            div(pre(log_content, style="white-space: pre-wrap; font-family: monospace; background: #f5f5f5; padding: 10px;"))

    if args.epi2me_log and os.path.exists(args.epi2me_log) and os.path.getsize(args.epi2me_log) > 0:
        with report.add_section("EPI2ME Backup Log", "EPI2ME Log"):
            with open(args.epi2me_log) as f:
                log_content = f.read()
            div(pre(log_content, style="white-space: pre-wrap; font-family: monospace; background: #f5f5f5; padding: 10px;"))

    report.write(args.report)
    logger.info(f"Report written to {args.report}.")


def argparser():
    """Argument parser for entrypoint."""
    parser = wf_parser("report")
    parser.add_argument("report", help="Report output file")
    parser.add_argument(
        "--ont_manifest",
        help="JSON manifest of ONT backup files")
    parser.add_argument(
        "--epi2me_manifest",
        help="JSON manifest of EPI2ME backup files")
    parser.add_argument(
        "--ont_log",
        help="Log file for ONT backup")
    parser.add_argument(
        "--epi2me_log",
        help="Log file for EPI2ME backup")
    parser.add_argument(
        "--versions", required=True,
        help="directory containing CSVs containing name,version.")
    parser.add_argument(
        "--params", required=True,
        help="A JSON file containing the workflow parameter key/values")
    parser.add_argument(
        "--wf_version", default='unknown',
        help="version of the executed workflow")
    return parser
