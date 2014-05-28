import sys
from works.OmvicScrapper import OmvicScrapper

__author__ = 'Rabbi'

if __name__ == "__main__":
    omvic = OmvicScrapper()
    omvic.scrapData(int(sys.argv[1]), int(sys.argv[2]))
