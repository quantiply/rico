import datetime
import time
import random
import json
import uuid
import urllib2


country_population = {"US": 7500, "UK": 1500, "AUS": 1000}

pages = {
    "trading": {"account_balance": 0.05, "view_trading_history": 0.15, "new_trade": 0.4, "view_portfolio": 0.2, "ticker": 0.2},
    "banking": {"account_balance": 0.3, "view_loan": 0.1, "transaction_history": 0.3, "transfer_money": 0.3},
    "insurance": {"pay_premium": 0.5, "add_member": 0.4, "view_account": 0.1},
    "general": {"login": 0.3, "logout": 0.3, "home": 0.3, "error": 0.1}
}

traffic_pattern = [
    {"start": 0, "end": 6, "data": {"US": 0.10, "UK": 0.05, "AUS": 0.25, "NOOP": 0.60}},
    {"start": 6, "end": 7, "data": {"US": 0.10, "UK": 0.05, "AUS": 0.50, "NOOP": 0.35}},  # 5pm in "AUS"
    {"start": 7, "end": 9, "data": {"US": 0.10, "UK": 0.15, "AUS": 0.20, "NOOP": 0.55}},
    {"start": 9, "end": 10, "data": {"US": 0.10, "UK": 0.45, "AUS": 0.05, "NOOP": 0.40}},  # 9am in "UK"
    {"start": 10, "end": 13, "data": {"US": 0.05, "UK": 0.15, "AUS": 0.05, "NOOP": 0.75}},
    {"start": 13, "end": 14, "data": {"US": 0.25, "UK": 0.15, "AUS": 0.05, "NOOP": 0.55}},
    {"start": 14, "end": 15, "data": {"US": 0.80, "UK": 0.15, "AUS": 0.05, "NOOP": 0}},  # 9 am EST, 2pm GMT , 1am AEDT
    {"start": 15, "end": 22, "data": {"US": 0.30, "UK": 0.15, "AUS": 0.15, "NOOP": 0.40}},  # Traffic dies down here.
    {"start": 22, "end": 23, "data": {"US": 0.57, "UK": 0.03, "AUS": 0.40, "NOOP": 0}},  # 5 pm EST, 11pm GMT , 9am AEDT
    {"start": 23, "end": 24, "data": {"US": 0.15, "UK": 0.05, "AUS": 0.25, "NOOP": 0.55}}
]

page_load_dist = {"mean": 2, "stddev": 4}
session_length = {"min": 2, "max": 20}


def weighted_choice(choices):
    total = sum([w for c, w in choices.iteritems()])
    r = random.uniform(0, total)
    upto = 0
    for c, w in choices.iteritems():
        if upto + w >= r:
            return c
        upto += w


def generate_session(user, length):
    session = []
    cookie_id = str(uuid.uuid1())
    product = random.choice(user["product"])
    device = random.choice(user["devices"])

    page = "NA"  # default value NA for referrer if it is the first click
    for i in range(length):
        referrer = page

        if random.random() > 0.9:
            page = weighted_choice(pages["general"])
        else:
            page = weighted_choice(pages[product["category"]])

        load_time = random.random() * page_load_dist["mean"] + page_load_dist["stddev"]

        click = {
            "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "user": user["username"],
            "segment": user["segment"],
            "page_title": page,
            "page_category": product["category"],
            "page_referrer": referrer,
            "cookieId": cookie_id,  # Call this session id
            "geo_country": user["country"],
            "geo_state": user["state"],
            "geo_latitude": user["geo"]["lat"],
            "geo_longitude": user["geo"]["lng"],
            "ip_isp": user["isp"]["name"],
            "ip_domain": user["isp"]["domain"],
            "ip_netspeed": user["isp"]["speed"],
            "browser": device["browser"],
            "os": device["os"],
            "device": device["type"],
            "loadtime": load_time
        }
        session.append(click)

    return session


def generate_sessions(users, length):

    traffic_pattern_exp = {hour: pat["data"]
                           for pat in traffic_pattern
                           for hour in range(pat["start"], pat["end"])}

    clicks = []
    while len(clicks) < length:
        now = datetime.datetime.now()

        country = weighted_choice(traffic_pattern_exp[now.hour])
        if country == "NOOP":
            continue

        population = country_population[country]
        user = users[country][random.randrange(0, population)]

        clicks += generate_session(user, random.randrange(session_length["min"],
                                   session_length["max"]))

    return clicks


def users_from_file(file_name):
    users = []
    with open(file_name) as f:
        for line in f:
            users.append(json.loads(line))
    return users


def post_to_url(data, url):
    req = urllib2.Request(url)
    req.add_header('Content-Type', 'application/json')
    response = urllib2.urlopen(req, json.dumps(data))
    print "%s clicks processed." % len(data)


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Clickstream generator.')
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
    datasource = args.datasource or "pageviews"
    debug = args.console
    url = "http://{host}:{port}/v1/post/{datasource}".format(
                host=host,
                port=port,
                datasource=datasource
    )

    users = dict()

    for country in ["US", "UK", "AUS"]:
        users[country] = users_from_file("data/%s.txt" % country)

    while True:
        clicks = generate_sessions(users, int(batch_size))
        if debug:
            for click in clicks:
                print json.dumps(click)
        else:
            post_to_url(clicks, url)

        time.sleep(int(delay_between_batches_secs))


if __name__ == "__main__":
    main()
