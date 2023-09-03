import argparse, io, sqlite3, os, markdown, json
import zstandard as zstd
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import multiprocessing as mp
from validators import url


DCTX = zstd.ZstdDecompressor(max_window_size=2**31)


def read_lines_from_zst_file(zstd_file_path: Path):
    DCTX = zstd.ZstdDecompressor(max_window_size=2**31)
    with (
        zstd.open(zstd_file_path, mode="rb", dctx=DCTX) as zfh,
        io.TextIOWrapper(zfh) as iofh,
    ):
        for line in iofh:
            yield line


def read_files(file_queue, input):
    while True:
        file_path = file_queue.get()
        if not file_path:
            break

        zst_file = Path(file_path)
        for record in read_lines_from_zst_file(zst_file):
            input.put([True, record])


def do_work(input, output):
    while True:
        command = input.get()
        if command[0] == False:
            break

        data = command[1]

        record = json.loads(data)
        submission = record.get("selftext")  # if submission
        comment = record.get("body")  # if comment

        raw_text = ""
        text_type = ""
        if submission:
            raw_text = submission
            text_type = "submission"
        if comment:
            raw_text = comment
            text_type = "comment"

        if not text_type:
            continue  # sloppy

        md = markdown.markdown(raw_text)
        soup = BeautifulSoup(md, "lxml")

        for link in soup.findAll("a"):
            link_text = link.string

            if not link_text:
                continue

            link_text = link_text.strip()

            if not link_text.startswith("http"):
                continue

            try:
                link_text_valid = url(link_text)
            except Exception:
                continue
            if not link_text_valid:
                continue

            link_text_domain = urlparse(link_text).netloc.lower()

            if not link_text_domain:
                continue

            link_goto = link.get("href")

            try:
                link_goto_valid = url(link_goto)
            except Exception:
                continue
            if not link_goto_valid:
                continue

            link_goto_domain = urlparse(link_goto).netloc.lower()

            if not link_goto_domain:
                continue

            if link_text_domain != link_goto_domain:
                save = {
                    "link_text": link_text,
                    "link_text_domain": link_text_domain,
                    "link_goto": link_goto,
                    "link_goto_domain": link_goto_domain,
                    "author": record.get("author"),
                    "subreddit": record.get("subreddit"),
                    "created": int(record.get("created_utc")),
                    "edited": int(record.get("edited")),
                    "text_type": text_type,
                }
                output.put(save)
                print(save)


def save_work(output, db_name):
    con = sqlite3.connect(db_name, isolation_level="DEFERRED")
    cur = con.cursor()

    cur.execute(
        "CREATE TABLE IF NOT EXISTS deceptive(link_text TEXT, link_text_domain TEXT, link_goto TEXT, link_goto_domain TEXT, author TEXT, subreddit TEXT, created INT, edited INT, text_type TEXT)"
    )
    con.commit()

    cur.execute("PRAGMA synchronous = OFF")
    cur.execute("PRAGMA journal_mode = OFF")

    while True:
        save = output.get()
        if not save:
            break

        insert = (
            save["link_text"],
            save["link_text_domain"],
            save["link_goto"],
            save["link_goto_domain"],
            save["author"],
            save["subreddit"],
            save["created"],
            save["edited"],
            save["text_type"],
        )

        cur.execute("INSERT INTO deceptive VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", insert)
        con.commit()


if __name__ == "__main__":
    description = "Looks for deceptive links in Pushshift dumps."

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--directory",
        required=True,
        help="The directory of Pushshift dump files to analyze.",
    )
    parser.add_argument(
        "--database",
        required=True,
        help="The the name of the database to save data to.",
    )
    parser.add_argument(
        "--reader-threads",
        required=False,
        type=int,
        default=1,
        help="The number of threads that should read zst files (recommended: 1-2, depending on single thread performance)",
    )
    parser.add_argument(
        "--worker-threads",
        required=False,
        type=int,
        default=1,
        help="The number of threads that should process text for deceptive links (recommended: num_cpus - reader_threads)",
    )
    args = parser.parse_args()

    # set up queues for handling cross-process communication, all size-limited for safety
    file_queue = mp.Queue(maxsize=1000)
    input = mp.Queue(maxsize=250000)
    output = mp.Queue(maxsize=250000)

    # start the database thread
    db = mp.Process(
        target=save_work,
        args=(
            output,
            args.database,
        ),
    )
    db.start()

    # start the readers
    readers = []
    for i in range(args.reader_threads):
        p = mp.Process(
            target=read_files,
            args=(
                file_queue,
                input,
            ),
        )
        p.start()
        readers.append(p)

    # pass all files to read to the reader processes
    for directory, subdirectories, files in os.walk(args.directory):
        for file in files:
            file_queue.put(os.path.join(directory, file))

    # start the workers
    analyzers = []
    for i in range(args.worker_threads):
        p = mp.Process(
            target=do_work,
            args=(
                input,
                output,
            ),
        )
        p.start()
        analyzers.append(p)

    # kill the reader threads
    for p in readers:
        file_queue.put(False)

    for p in readers:
        p.join()

    # kill the worker threads
    for p in analyzers:
        input.put([False])

    for p in analyzers:
        p.join()

    # kill the database thread
    output.put(False)
    db.join()
