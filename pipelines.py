from downloader import GoogleDriveDownloader
from reader import Reader
from database import Database
from core import Verbose
from config import args
from decorators import message


@message("Starting Downloader Pipeline")
def downloader_pipeline(verbose: bool = args.verbose) -> list:
    downloader = GoogleDriveDownloader(verbose=verbose)
    link_eans = downloader.make_link(downloader.id_eans)
    link_data = downloader.make_link(downloader.id_data)
    return [
            downloader.download(link_eans, downloader.files['eans']),
            downloader.download(link_data, downloader.files['data'])
            ]


@message("Starting Reader Pipeline")
def reader_pipeline(verbose: bool = args.verbose) -> list:
    reader = Reader(verbose=verbose)
    eans = reader.read_eans()
    products = reader.read_data(eans)
    Verbose.print(reader.print, products)
    return products


@message("Starting Database Pipeline")
def database_pipeline(csv_records: list, table: str = 'gcn', verbose: bool = args.verbose) -> None:
    database = Database(verbose=verbose)
    if not database.table_exists(table):
        database.create_table(table)
    records_to_add = database.compare_records(csv_records, table)
    if records_to_add:
        database.add_records(records_to_add, csv_records, table)
