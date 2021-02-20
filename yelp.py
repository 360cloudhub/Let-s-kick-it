import requests
from web.models import ApiKeys

def __search(params):
    api_key = ''
    for keys in ApiKeys.select():
        api_key = keys.key
                        
    s = requests.session()
    s.headers = {
        "Authorization": "Bearer " + api_key
    }
    r = s.get("https://api.yelp.com/v3/businesses/search", params=params)
    ratelimits = ApiKeys.select()
    if ratelimits:
        ratelimits = ApiKeys.select().get()
        ratelimits.ratelimit = r.headers['ratelimit-remaining']
    else:
        ratelimits = ApiKeys(r.headers['ratelimit-remaining'])
    ratelimits.save()
    return r.json()["businesses"]


def search(latitude, longitude, radius, category='[]'):
    results = []

    search_params = ({
        "term": "restaurants",
        "limit": 50,
        "latitude": latitude,
        "longitude": longitude,
        "radius": radius,
        "categories": ",".join(list(map(lambda c: c, eval(category)))),
    })

    while True:
        page = __search(search_params)
        results.extend(page)
        if len(page)<50:
            # print(search_params)
            break
        search_params.update({
            "offset": search_params.get("offset", 0) + 50
        })

    return results


def search_all_circles(circles, start_index=0, category='[]'):
    print('start_index', start_index)
    data = []
    uniq_set = set()
    for i,circle in enumerate(circles[start_index:]):
        try:
            results = search(circle[0][1],circle[0][0],circle[1],category)
        except:
            return False, data, start_index + i

        for j in results:
            if j['id'] not in uniq_set:
                data.append(j)
                uniq_set.add(j['id'])
    return True, data, start_index + i 

# if __name__ == "__main__":
#     print(len(search("Los Angeles, CA")))
