import numpy as np
import models as m
import matplotlib
import matplotlib.pyplot as plt
from prettyprint import pp
from sqlalchemy import func

np.set_printoptions(linewidth=160)


P = np.matrix([[0, 1.0/2, 1.0/2, 0, 0, 0],
              [0, 0, 0, 0, 0, 0],
              [1.0/3, 1.0/3, 0, 0, 1.0/3, 0],
              [0, 0, 0, 0,  1.0/2, 1.0/2],
              [0, 0, 0, 1.0/2, 0, 1.0/2],
              [0, 0, 0, 1, 0, 0]])

n = P.shape[0]
V = np.matrix([1.0/n for _ in xrange(n)])


def load_data():
    """
    Loads data from database
    """
    s = m.Session()
    id_to_i = {}
    total_links = {}
    LIMIT = 13000
    # Select top LIMIT nodes ordered by number of inlinks
    pages = s.query(m.Page)\
             .join(m.Link, m.Page.id == m.Link.to_id)\
             .group_by(m.Page.id)\
             .order_by('COUNT(*) DESC')\
             .limit(LIMIT)\
             .all()

    n = len(pages)
    P = [[0 for _ in xrange(n)] for _ in xrange(n)]

    print "Generating identity map"
    cnt = 0
    for page in s.query(m.Page).order_by(m.Page.id).limit(LIMIT).all():
        id_to_i[page.id] = cnt
        cnt += 1
        linknum = 0
        for link in page.outlinks:
            linknum += link.n

        total_links[page.id] = linknum

    print "Identity map generated"
    "Generating P"
    for link in s.query(m.Link).all():
        if link.from_id in id_to_i and link.to_id in id_to_i:
            P[id_to_i[link.from_id]][id_to_i[link.to_id]] =\
              float(link.n) / total_links[link.from_id]

    print "P generated"
    n = len(P)
    V = np.matrix([1.0/n for _ in xrange(n)])
    print "Closing session..."
    print "generating matrix"
    P = np.matrix(P)
    return P
    print "Dvocrtica"
    P = dvocrtica(P, V)

    print "Doing the pagerank.."
    res = do_the_pagerank(P, V)
    V = res['V']
    for i in np.nditer(np.argsort(V)):
        i = int(i)
        print pages[i].url, V[0, i], pages[i].id

    s.close()

def dvocrtica(P, V, ALPHA=0.85):
    """
    Converts P to primitive matrix
    """
    e = np.ones_like(V).T
    a = np.matrix([int(x) for x in np.nditer(~P.any(axis=1))]).T
    return ALPHA * P + (ALPHA * a + (1 - ALPHA) * e) * V


def iteration(P, V):
    return P * V


def distance(A, B):
    """
    Defines distance between A and B
    """
    total_sum = 0
    for i in xrange(A.shape[1]):
        total_sum += abs(A[0, i] - B[0, i])

    return total_sum


def normalize(V):
    """
    Normalizes vector V
    """
    norm = 0
    for i in xrange(V.shape[1]):
        norm += abs(V[0, i])

    return V/float(norm)


def do_the_pagerank(P, V=None, tolerance=pow(10, -6), iterations=200,
                    criteria="iterations"):
    """
    Calculates the pagerank vector.
    """
    if V is None:
        n = P.shape[0]
        V = np.matrix([1.0/n for _ in xrange(n)])

    previous_V = normalize(np.matrix([i for i in xrange(P.shape[0])]))
    iter_num = 0
    d = distance(V, previous_V)
    residual = [d]
    while True:
        iter_num += 1
        previous_V = V
        V = normalize(iteration(V, P))

        d = distance(V, previous_V)
        residual.append(d)

        if criteria == "tolerance" and d < tolerance:
            break
        if criteria == "iterations" and iter_num >= iterations:
            break

    return {
        "V": V,
        "residual": residual,
        "iter_num": iter_num,
    }


# Alphas used for analysis
ALPHAS = [0.5, 0.75, 0.8, 0.85, 0.9, 0.95, 0.98, 0.99]


def analyze_pagerank():
    ANALYZE_ALPHA = True

    P = load_data()
    n = P.shape[0]
    V = np.matrix([1.0/n for _ in xrange(n)])
    if ANALYZE_ALPHA:
        print "Analyzing alphas:", ALPHAS
        res = []
        for alpha in ALPHAS:
            v = do_the_pagerank(dvocrtica(P, V, ALPHA=alpha), V, criteria="tolerance")
            print alpha, " && ", v['iter_num']

        res = res
        plot_graph(res, 'test.png', field="residual",
                   xlabel="Iteration #", ylabel="Residual value",
                   line_label="alpha=%s",
                   line_label_params="(ALPHAS[i])")


def plot_graph(data, filename, field="residual", xlabel="", ylabel="",
               line_label=None, line_label_params=None):
    y_formatter = matplotlib.ticker.ScalarFormatter(useOffset=False)

    COLORS = ['red', 'blue', 'green', 'magenta', 'cyan']
    x = [i for i in xrange(len(data[0]['residual']))]
    ax = plt.gca()
    ax.yaxis.set_major_formatter(y_formatter)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.axis([0, 200, 0, pow(10, -5)])
    for i in xrange(len(data)):
        if line_label and line_label_params:
            plt.plot(x, data[i][field], color=COLORS[i],
                     label=line_label % eval(line_label_params))
        else:
            plt.plot(x, data[i][field], color=COLORS[i])
    if field == "residual":
        plt.plot(x, [pow(10, -6) for _ in xrange(len(x))], color='black',
                 label="tolerance=10^(-6)")
    plt.legend(loc='best')
    plt.savefig(filename)
    plt.clf()


if __name__ == "__main__":
    #load_data()
    analyze_pagerank()
