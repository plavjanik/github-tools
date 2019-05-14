import argparse
import json
import statistics
import sys
from collections import defaultdict, namedtuple
from datetime import datetime
from typing import DefaultDict, Dict, List, NamedTuple

import arrow

parser = argparse.ArgumentParser(
    description="Parses the output of download_data.py into a list of reviews and their status, either 'on_time', 'late', or 'no_response'"
)
parser.add_argument("-f", "--input-file",
                    help="file to parse; if omitted uses stdin")
parser.add_argument("-o", "--output-file",
                    help="file to output; if omitted uses stdout")
parser.add_argument("-tz", default="Europe/Prague",
                    help="timezone to use for calculating business hours for review status")
args = parser.parse_args()

if args.input_file:
    with open(args.input_file, 'r') as f:
        data = json.load(f)
else:
    data = json.load(sys.stdin)

reviews = []
for pr in data:
    review_requested = review_completed = pr_completed = None

    # raw data transformation step
    for item in pr['timelineItems']['nodes']:
        # transform all datetime strings into localized arrow datetime objects
        if 'createdAt' in item:
            item['createdAt'] = arrow.get(item['createdAt']).to(args.tz)
        if 'submittedAt' in item:
            item['submittedAt'] = arrow.get(item['submittedAt']).to(args.tz)

    # computation step
    for item in pr['timelineItems']['nodes']:
        typename = item['__typename']
        if typename == 'ReviewRequestedEvent':
            if not 'login' in item['requestedReviewer']:
                continue

            time = item['createdAt']

            if not review_requested or time < review_requested:
                review_requested = time

        elif typename == 'PullRequestReview':
            time = item['submittedAt']

            if not review_completed or time > review_completed:
                review_completed = time

        elif typename == 'ReviewRequestRemovedEvent':
            if not 'login' in item['requestedReviewer']:
                continue

            reviewer = item['requestedReviewer']['login']
            time = item['createdAt']

            if not review_completed or time > review_completed:
                review_completed = time

        elif typename in ['ClosedEvent', 'MergedEvent']:
            time = item['createdAt']

            if not pr_completed or time > pr_completed:
                pr_completed = time

        else:
            print(f"Unknown type: {typename}", file=sys.stderr)

        pr_duration = (pr_completed - review_requested).total_seconds() / \
            3600 if pr_completed and review_requested else None
        review_duration = (review_completed - review_requested).total_seconds() / \
            3600 if review_completed and review_requested else None
        if pr_duration and review_duration:
            reviews.append({"pr_duration": pr_duration,
                            "review_duration": review_duration})

output_file = open(args.output_file, 'w') if args.output_file else sys.stdout
output_file.write(json.dumps(reviews, indent=2) + "\n")

print("Mean review time: %.1f" % statistics.mean(
    [r["review_duration"] for r in reviews]))
