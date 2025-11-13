#!/usr/bin/env python3

import csv
import json
import requests
from argparse import ArgumentParser
from pathlib import Path

class Process:
    def __init__(self):
        argparser = ArgumentParser(description="fetch and summarize medical data")

        argparser.add_argument("-o", "--outfile", type=Path)
        argparser.add_argument("-u", "--url", default="http://127.0.0.1:8000")
        argparser.add_argument("-v", "--verbose", action="store_true")
        argparser.add_argument("exportID")

        self.args = argparser.parse_args()
        self.exportID = self.args.exportID

        if self.args.outfile is None:
            self.args.outfile = Path(f"{self.exportID}.json")

    def mkapiurl(self, path):
        return f"{self.args.url}/api/{path}"

    def error(self, message):
        print(message)
        exit(1)

    @staticmethod
    def getdata(response, path):
        json = response.json()

        if "data" not in json:
            self.error(f"data not in response")

        data = json["data"]

        if path not in data:
            self.error(f"{path} not in response")

        return data[path]

    def get(self, path, stream=False):
        response = requests.get(self.mkapiurl(path), stream=stream)

        if not response.ok:
            self.error(f"can't reach URL {self.args.url}")

        return response

    def vprint(self, msg):
        if not self.args.verbose:
            return

        print(msg)

    def writedata(self):
        if self.patient_id is None:
            return

        if self.firstpatient:
            self.firstpatient = False
        else:
            # add comma for next JSON record
            self.write(',')

        self.write(f'    "{self.patient_id}":', end='')
        self.write(json.dumps(self.patientdata, indent=2), end='')

    def write(self, msg, end='\n'):
        print(msg, file=self.ofp, end=end)

    def process(self):
        # initialize field counts
        response = self.get("export")

        # check if export ID is valid
        export_ids = self.getdata(response, "export_ids")

        if self.exportID not in export_ids:
            self.error(f"exportID {self.exportID} not in {export_ids}")

        # fetch download IDs for export
        response = self.get(f"export/{self.exportID}")
        download_ids = self.getdata(response, "download_ids")

        self.vprint(f"Got {len(download_ids)} download IDs")

        # initialize reading counts
        counts = {}
        self.firstpatient = True

        # set up output file for writing
        self.ofp = self.args.outfile.open("w")
        self.write('{')
        self.write('  "patients": {')

        # iterate through download IDs, fetching and processing each
        for download_id in download_ids:
            self.vprint(f"fetching download ID {download_id}")

            response = self.get(f"export/{self.exportID}/{download_id}/data", stream=True)
            reader = csv.DictReader(response.iter_lines(decode_unicode=True))
            nrows = 0
            self.patient_id = None
            self.patientdata = {}

            for row in reader:
                if row["patient_id"] != self.patient_id or row["event_type"] in self.patientdata:
                    # new patient or repeated data, start new record
                    self.writedata()

                self.patient_id = row["patient_id"]
                self.patientdata[row["event_type"]] = row["value"]

                # count reading type
                if row["event_type"] not in counts:
                    counts[row["event_type"]] = 0

                counts[row["event_type"]] += 1
                
                nrows += 1

            # write final record (if any)

            self.writedata()

            self.vprint(f"{nrows} rows read")

        # write totals
        self.write('  },')
        self.write('  "totals":', end='')
        self.write(json.dumps(counts, indent=2))
        self.write('}')

        self.ofp.close()
        

def main():
    proc = Process()

    proc.process()
