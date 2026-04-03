from pprint import pprint

from common import fetch_pending_reviews

if __name__ == "__main__":
    reviews, count_unanswered, count_archive = fetch_pending_reviews(take=5)
    print("Неотвеченных:", count_unanswered)
    print("Архив:", count_archive)
    for review in reviews:
        pprint(review)
        print("-" * 80)
