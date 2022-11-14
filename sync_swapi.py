import requests
import datetime


def get_people(people_id):

    return requests.get(f'https://swapi.dev/api/people/{people_id}').json()

def main():
    for i in range(1, 11):
        print(get_people(people_id=i))

start = datetime.datetime.now()
main()
print(datetime.datetime.now() - start)
