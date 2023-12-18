import sys
import re
reload(sys)
sys.setdefaultencoding('utf-8') # required to convert to unicode

for line in sys.stdin:
    try:
        article_id, text = unicode(line.strip()).split('\t', 1)
    except ValueError as e:
        continue
    words = re.sub('[^A-Za-z\\s]', '', text.lower(), flags=re.UNICODE).split()
    sorted_words = []
    for word in words:
        if len(word) >= 3:
            sorted_words.append([''.join(sorted(word)), word])
    for word in sorted_words:
        print "%s\t%s\t%d" % (word[0], word[1], 1)

