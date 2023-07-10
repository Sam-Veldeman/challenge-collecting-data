from Utils.async_scrape import run_scraper
import asyncio

def get_num_pages():
    while True:
        try:
            num_pages = int(input("Enter the number of pages to scrape: "))
            if num_pages > 0:
                return num_pages
            else:
                print("Number of pages should be greater than 0.")
        except ValueError:
            print("Invalid input. Please enter a valid number.")

def main():
    num_pages = get_num_pages()
    asyncio.run(run_scraper(num_pages))

if __name__ == "__main__":
    main()