"""Count words."""
import operator
from collections import Counter

def count_words(s, n):
    """Return the n most frequently occuring words in s."""
    
    # TODO: Count the number of occurences of each word in s
    z = s.split(' ')
    #from collections import Counter
    top_n = Counter(z)
    
    # TODO: Sort the occurences in descending order (alphabetically in case of ties)
    
    # TODO: Return the top n most frequent words.
    top_n = sorted(top_n.items(), key=operator.itemgetter(0), reverse=False)
    top_n = sorted(top_n, key=operator.itemgetter(1), reverse=True)
    return top_n[:n]


def test_run():
    """Test count_words() with some inputs."""
    print(count_words("cat bat mat cat bat cat", 3))
    print(count_words("betty bought a bit of butter but the butter was bitter", 3))


if __name__ == '__main__':
    test_run()
