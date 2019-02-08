from urlparse import urlparse, urljoin
import yaml
conf_f = file('./conf.yaml', 'r')
conf = yaml.load(conf_f)['spider']
conf_f.close()


def fullPath(parent, link):
    # For constructing simple links we can ignore everything after
    # last slash, e.g. on http://test.com/super.ext?mag=ic .
    # Relative links on that page will be constructed with
    # http://test.com/ prefix
    link = link.strip()

    for string in ['http', 'https']:
        if link.startswith(string):
            '''Full link'''
            return link

    for string in ['javascript', 'mailto']:
        if link.startswith(string):
            '''We are not interested in following these links'''
            return None

    if link.startswith('/'):
        parsed_uri = urlparse(parent)
        domain = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)
        return domain + link

    if '#' in link:
        link = link[:link.find('#')]

    return urljoin(parent, link)
