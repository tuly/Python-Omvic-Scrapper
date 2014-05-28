import urllib
import re


def alexa_find(match):
    htmlfile = urllib.urlopen(match)
    htmltext = htmlfile.read()
    htmltext = re.sub('\n+', ' ', htmltext)
    htmltext = re.sub('\s+', ' ', htmltext)
    these_regex = '<strong class="metrics-data align-vmiddle">\s*?([0-9,]+)\s*?</strong>'
    pattern = re.compile(these_regex)
    rank = re.findall(pattern, htmltext)

    if len(rank) > 0:
        print re.sub(',', '', rank[0])
    else:
        print -1


user_input = "http://www.alexa.com/siteinfo/" + raw_input("input url for alexa ranking: ")
alexa_find(user_input)