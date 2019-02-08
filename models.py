from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy import ForeignKey, DateTime
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

import yaml
conf_f = file('./conf.yaml', 'r')
db_conf = yaml.load(conf_f)['database']
conf_f.close()

engine = create_engine(
    '%s://%s:%s@%s/%s' % (
        db_conf['backend'], db_conf['user'], db_conf['password'],
        db_conf['host'], db_conf['db_name']
    ), echo=False, pool_size=100, pool_recycle=25000)


Session = sessionmaker(bind=engine)
Base = declarative_base()


class Page(Base):
    __tablename__ = 'page'

    PENDING = 1
    COMPLETED = 2
    FAILED = 3

    id = Column(Integer, primary_key=True)
    url = Column(String(255), unique=True)
    last_update = Column(DateTime, default=None)
    parse_status = Column(Integer, default=PENDING)

    def __repr__(self):
        print "O"
        print self.id
        return "<Page[%s](url=%s, last_update=%s)>" % (
            self.id, self.url, self.last_update
        )

    def __str__(self):
        return self.__repr__()


class Link(Base):
    __tablename__ = 'link'

    from_id = Column(Integer, ForeignKey('page.id'), primary_key=True)
    to_id = Column(Integer, ForeignKey('page.id'), primary_key=True)
    n = Column(Integer, default=1)

    from_page = relationship(
        'Page', backref=backref('outlinks', order_by="desc(Link.n)"),
        foreign_keys=from_id
    )
    to_page = relationship(
        'Page', backref=backref('inlinks', order_by="desc(Link.n)"),
        foreign_keys=to_id
    )


    def __repr__(self):
        return "<Link[%s, %s](from_page=%s, to_page=%s)>" % (
            self.from_id, self.to_id, self.from_page, self.to_page
        )

    def __str__(self):
        return self.__repr__()


def number_of_dangling_nodes():
    s = Session()
    print s.query(Page).filter(Page.outlinks == 0).count()

if __name__ == '__main__':
    Base.metadata.create_all(engine)
