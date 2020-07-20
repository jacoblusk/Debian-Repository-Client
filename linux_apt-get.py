import argparse
import urllib.parse
import urllib.request
import os.path
import sqlite3
import gzip

PACKAGES_FILE = "Packages.gz"

parser = argparse.ArgumentParser()
parser.add_argument("--distribution", "-d", default="stable")
parser.add_argument("--component", "-c",
                    choices=["main", "contrib", "non-free"], default="main")
parser.add_argument(
    "--search", "-s", help="to search the repositrory", action="store_true")
parser.add_argument(
    "--get", "-g", help="download binary associated with the supplied package", action="store_true")
parser.add_argument("package", help="package to search for", metavar="P",
                    type=str)
parser.add_argument("-a", "--arch", help="package architecture", default="amd64",
                    type=str)
args = parser.parse_args()
print(args)


def packages_report_hook(chunk_number, max_size_chunk, total_size):
    total_chunks = total_size // max_size_chunk + 1
    print(f"Downloading {PACKAGES_FILE} ("
          f"{(chunk_number * max_size_chunk) // 1024}KB /"
          f"{total_size // 1024}KB)",
          end='\r', flush=True)


def read_packages(file):
    package, packages = {}, {}
    while (line := f.readline().decode('utf-8')) != "":
        if line == "\n":
            packages[package["Package"]] = package
            package = {}
            continue
        line = line.rstrip()
        field, value = line.split(':', 1)
        package[field.lstrip()] = value.lstrip()
    return packages


def create_package_insert_query(package):
    keys = package.keys()
    heading = ",".join([f"\"{key}\"" for key in keys])
    values = [package[key] for key in keys]
    insert_query = (
        f"INSERT INTO packages ({heading})"
        f"VALUES({','.join(['?' for key in keys])})"
    )

    return insert_query, tuple(values)


def create_packages_table_query(packages):
    keys = set()
    for package_name, package in packages.items():
        keys.update(package.keys())
    keys = list(keys)
    headings = ",".join([f"\"{key}\" text" for key in keys])
    create_table_query = f"CREATE TABLE packages ({headings});"
    return create_table_query


if __name__ == "__main__":
    packages_url = urllib.parse.urlunparse((
        "http", "ftp.us.debian.org",
        f"debian/dists/{args.distribution}/{args.component}/binary-{args.arch}/{PACKAGES_FILE}",
        "", "", ""))

    filename = os.path.join("/", "tmp", f"{args.distribution}_{args.component}_"
                                        f"{args.arch}_{PACKAGES_FILE}")
    db_filename = os.path.join("/", "tmp", f"{args.distribution}_{args.component}_"
                                           f"{args.arch}_{PACKAGES_FILE}.db")

    if not os.path.isfile(filename):
        _, headers = urllib.request.urlretrieve(
            packages_url,
            filename=filename,
            reporthook=packages_report_hook
        )

        with gzip.open(filename, 'rb') as f:
            packages = read_packages(f)

        create_table_query = create_packages_table_query(packages)
        with sqlite3.connect(db_filename) as conn:
            c = conn.cursor()
            c.execute(create_table_query)
            for package_name, package in packages.items():
                query, values = create_package_insert_query(package)
                c.execute(query, values)
            conn.commit()

    if args.search:
        with sqlite3.connect(db_filename) as conn:
            c = conn.cursor()
            result = c.execute("SELECT Package FROM packages WHERE Package LIKE ?;",
                               (f"%{args.package}%",))
            for row in result:
                print(row)
