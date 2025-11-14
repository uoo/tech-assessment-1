#!/usr/bin/env python3

import csv
import json
import requests
import dateutil.parser
from argparse import ArgumentParser
from pathlib import Path

# iterator to parse and provide rows of readings from an input stream
class InputStream:
    def __init__(self, response):
        self.reader = csv.DictReader(response.iter_lines(decode_unicode=True))

    def __iter__(self):
        return self

    def __next__(self):
        row = next(self.reader)
        # parse timestamp for ordering
        row["timestamp"] = dateutil.parser.isoparse(row["event_time"])
        # parse value so it's not stored as a string in JSON
        row["value"] = int(row["value"])

        return row


# iterator to return results from a collection of streams, ordered by time
class Multi:
    def __init__(self, streams):
        # initialize sorted list of data
        datas = []

        for stream in streams:
            try:
                row = next(stream)
                datas.append({
                    'stream': stream,
                    'row': row,
                })
            except StopIteration:
                pass

        self.datas = self.timesort(datas)

    def __iter__(self):
        return self

    def __next__(self):
        if len(self.datas) == 0:
            # no more data, we're done
            raise StopIteration

        # grab data from row with earliest time
        firstrow = self.datas[0]['row']

        # update sorted data array
        try:
            # update with next row from same stream
            self.datas[0]['row'] = next(self.datas[0]['stream'])

            # re-sort
            self.datas = self.timesort(self.datas)
        except StopIteration:
            # no more data from that stream, remove from list
            self.datas.pop(0)

        return firstrow

    @staticmethod
    def timesort(array):
        return sorted(array, key=lambda r: r['row']['timestamp'])


# overall class to process data
class Process:
    def __init__(self):
        argparser = ArgumentParser(description="fetch and summarize medical data")

        argparser.add_argument("-o", "--outfile", type=Path)
        argparser.add_argument("-u", "--url", default="http://127.0.0.1:8000")
        argparser.add_argument("-n", "--npatients", type=int)
        argparser.add_argument("-v", "--verbose", action="store_true")
        argparser.add_argument("exportID")

        self.args = argparser.parse_args()
        self.exportID = self.args.exportID
        self.datas = None

        if self.args.outfile is None:
            self.args.outfile = Path(f"{self.exportID}.json")

    # create URL to point to server API endpoint
    def mkapiurl(self, path):
        return f"{self.args.url}/api/{path}"

    # print error message and exit with non-zero status code
    def error(self, message):
        print(message)
        exit(1)

    # fetch data from response with error checking
    @staticmethod
    def getdata(response, path):
        json = response.json()

        if "data" not in json:
            self.error(f"data not in response")

        data = json["data"]

        if path not in data:
            self.error(f"{path} not in response")

        return data[path]

    # perform get request to API path
    def get(self, path, stream=False):
        response = requests.get(self.mkapiurl(path), stream=stream)

        if not response.ok:
            self.error(f"can't reach URL {self.args.url}")

        return response

    # print message if verbose is enabled
    def vprint(self, msg):
        if not self.args.verbose:
            return

        print(msg)

    # write a record of patient data to output file
    def writedata(self, patient_id, data):
        if self.firstpatient:
            self.firstpatient = False
        else:
            # add comma for next JSON record
            self.write(',')

        del data["timestamp"]

        self.write(f'    "{patient_id}":', end='')
        self.write(json.dumps(data, indent=2), end='')

    # send a string to the output file
    def write(self, msg, end='\n'):
        print(msg, file=self.ofp, end=end)

    # entry point to actuall process data
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

        # set up output file for writing and write header
        self.ofp = self.args.outfile.open("w")
        self.write('{')
        self.write('  "patients": {')

        # compute max number of patients to accumulate data for at once
        if self.args.npatients is None:
            # no number of patients specified, use number of download IDs
            npatients = len(download_ids)
        else:
            # use number of patients specified
            npatients = self.args.npatients

        # open data streams and instantiate fetchers
        istreams = []

        for download_id in download_ids:
            self.vprint("opening download ID {download_id}")

            response = self.get(f"export/{self.exportID}/{download_id}/data", stream=True)

            istreams.append(InputStream(response))

        # instantiate time-sorted multi-stream
        multi = Multi(istreams)

        # process patient data
        patients = {}
        nrows = 0

        # iterate through all rows from all datastreams
        for row in multi:
            # count readings
            if row["event_type"] not in counts:
                counts[row["event_type"]] = 0

            counts[row["event_type"]] += 1
                
            nrows += 1
            patient_id = row["patient_id"]

            if patient_id in patients:
                # we already have data for this patient
                if row["event_type"] in patients[patient_id]:
                    # duplicate event for same patient: emit previous row
                    self.writedata(patient_id, patients[patient_id])

                    # start new record
                    patients[patient_id] = {
                        row["event_type"]: row["value"],
                        "timestamp": row["timestamp"],
                    }
                else:
                    # existing patient, new event, update record
                    patients[patient_id][row["event_type"]] = row["value"]
                    patients[patient_id]["timestamp"] = row["timestamp"]
                    
            else:
                # new patient, make sure there's room

                if len(patients) > npatients:
                    # full, find oldest record

                    ots = None

                    for pt, data in patients.items():
                        if ots is None or data["timestamp"] < ots:
                            ots = data["timestamp"]
                            opt = pt

                    # emit that record
                    self.writedata(opt, patients[opt])

                    # remove from dict
                    del patients[opt]

                # start new record
                patients[patient_id] = {
                    row["event_type"]: row["value"],
                    "timestamp": row["timestamp"],
                }
                    
        # read all data, write final records

        for pt, data in patients.items():
            self.writedata(pt, data)
            
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
