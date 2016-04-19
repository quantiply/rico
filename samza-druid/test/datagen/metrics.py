import datetime
import time
import random
import json
import urllib2

urls = ["/foo/bar", "/", "/foo/baz", "/foo/buzz"]
users = ["alice", "bob", "charles"]
page_load_dist = {"mean": 2, "stddev": 4}


def generate_metrics():
    timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    metric = dict()
    # {"time": "2000-01-01T00:00:00Z", "url": "/foo/bar", "user": "alice", "latencyMs": 32}
    metric["time"] = timestamp
    metric["url"] = random.choice(urls)
    metric["user"] = random.choice(users)
    metric["latencyMs"] = int(random.random() * page_load_dist["mean"] + page_load_dist["stddev"])

    return metric


def post_to_url(data, url):
    req = urllib2.Request(url)
    req.add_header('Content-Type', 'application/json')
    response = urllib2.urlopen(req, json.dumps(data))
    print "%s metrics processed." % len(data)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Metrics generator.')
    parser.add_argument('-b', '--batch', help='Batch size')
    parser.add_argument('-t', '--delay', help='Delay b/w batches in secs')
    parser.add_argument('-s', '--host', help='Host')
    parser.add_argument('-p', '--port', help='Port')
    parser.add_argument('-d', '--datasource', help='Datasource')
    parser.add_argument('-c', '--console', help='Print to STDOUT.', action='store_true', default=False)

    args = parser.parse_args()

    batch_size = args.batch or 100
    delay_between_batches_secs = args.delay or 30
    host = args.host or 'localhost'
    port = args.port or 8200
    datasource = args.datasource or "metrics"
    debug = args.console
    url = "http://{host}:{port}/v1/post/{datasource}".format(
                host=host,
                port=port,
                datasource=datasource
    )

    while True:
        metrics = [generate_metrics() for count in xrange((int(batch_size)))]
        if debug:
            for metric in metrics:
                print json.dumps(metric)
        else:
            post_to_url(metrics, url)

        time.sleep(int(delay_between_batches_secs))


if __name__ == "__main__":
    main()
