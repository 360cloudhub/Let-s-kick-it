from web.models import YelpSearch, YelpRecord, Job
import time
import yelp
from yelp_circles import ComputeYelpCircles
import datetime
while True:
    for j in Job.select().where(Job.kind == "yelp", Job.state != "complete"):
        try:
            search = list(j.yelp_searches)[0]
            if j.state == "new":
                circle_scraper = ComputeYelpCircles(search.county_id, search.category)
                try:
                    circles = circle_scraper.get_circles()
                    j.state = "intermediate"
                    j.save()
                    search.circles = str(circles)
                    search.save()
                except Exception as e:
                    pass
            else:
                completed, results, completed_till = yelp.search_all_circles(eval(search.circles), search.start_index, search.category)
                all_records = list(map(lambda r: {
                        "search": search,
                        "yelp_id": r["id"],
                        "is_closed": r["is_closed"],
                        "name": r["name"],
                        "phone": r["phone"],
                        "street": r["location"]["address1"],
                        "city": r["location"]["city"],
                        "state": r["location"]["state"],
                        "zip_code": r["location"]["zip_code"],
                        "country": r["location"]["country"],
                        "url": r["url"],
                        "rating": r["rating"],
                        "review_count": r["review_count"],
                        "price_range": r.get("price", None),
                        # TODO: Is this okay?
                        "categories": ", ".join(list(map(lambda c: c["title"], r["categories"]))),
                        "order_type": ", ".join(r["transactions"]),
                        "is_chain": False
                    }, results))

                if len(all_records):
                    prev_records = list(YelpRecord.select().where(YelpRecord.search==search))
                    prev_records = [x.yelp_id for x in prev_records]
                    new_records = []
                    # print("DEDUPING")
                    for x in all_records:
                        if x['yelp_id'] not in prev_records:
                            new_records.append(x)
                    # print(len(all_records))
                    # print(len(new_records))
                    if len(new_records):
                        YelpRecord.insert_many(new_records).execute()

                if completed:
                    j.state = "complete"
                    j.finished = datetime.datetime.utcnow()
                    j.save()
                    search.start_index = completed_till+1
                    search.save()
                else:
                    search.start_index = completed_till
                    search.save()
        except IndexError:
            pass
    #     if search.search_type == "text":
    #         results = yelp.search(search.location_string)
    #     else:
    #         results = yelp.search(latitude=search.latitude, longitude=search.longitude, radius=search.radius, category=search.category)

    print("done")
    time.sleep(5)
