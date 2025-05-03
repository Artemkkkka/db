from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER
from sqlalchemy import Column, DateTime, Integer, Numeric, String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datetime import datetime, timezone
from typing import Optional


DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
Session = sessionmaker(bind=engine)

BaseModel = declarative_base()


class Genre(BaseModel):
    __tablename__ = 'genre'

    genre_id = Column(Integer, primary_key=True)
    name_genre = Column(String, nullable=False, unique=True)

    books = relationship("Book", back_populates="genre")


class Author(BaseModel):
    __tablename__ = 'author'

    author_id = Column(Integer, primary_key=True)
    name_author = Column(String, nullable=False)

    books = relationship("Book", back_populates="author")


class Book(BaseModel):
    __tablename__ = 'book'

    book_id = Column(Integer, primary_key=True)
    title = Column(String)
    author_id = Column(
        Integer,
        ForeignKey('author.author_id', ondelete='CASCADE'),
        nullable=False
    )
    genre_id = Column(
        Integer,
        ForeignKey('genre.genre_id', ondelete='CASCADE'),
        nullable=False
    )
    price = Column(Numeric(10, 2), nullable=False)
    amount = Column(Integer, nullable=False, default=0)


class City(BaseModel):
    __tablename__ = 'city'

    city_id = Column(Integer, primary_key=True)
    name_city = Column(String, nullable=False)
    days_delivery = Column(Integer, nullable=False)

    clientes = relationship("Client", back_populates="city")


class Client(BaseModel):
    __tablename__ = 'client'

    client_id = Column(Integer, primary_key=True)
    name_client = Column(String, nullable=False)
    city_id = Column(
        Integer,
        ForeignKey('city.city_id', ondelete='CASCADE'),
        nullable=False
    )
    email = Column(String, nullable=False, unique=True)

    buy = relationship("Buy", back_populates="client")


class Buy(BaseModel):
    __tablename__ = 'buy'

    buy_id = Column(Integer, primary_key=True)
    buy_description = Column(String, nullable=False)
    client_id = Column(
        Integer,
        ForeignKey('client.client_id', ondelete='CASCADE'),
        nullable=False
    )

    buy_steps = relationship("Buy_Step", back_populates="buy_for_step")
    buy_books = relationship("Buy_Book", back_populates="buy_for_books")


class Step(BaseModel):
    __tablename__ = 'step'

    step_id = Column(Integer, primary_key=True)
    name_step = Column(String, nullable=False, unique=True)

    books = relationship("Buy_Step", back_populates="step")


class Buy_Step(BaseModel):
    __tablename__ = 'buy_step'

    buy_step_id = Column(Integer, primary_key=True)
    buy_id = Column(
        Integer,
        ForeignKey('buy.buy_id', ondelete='CASCADE'),
        nullable=False
    )
    step_id = Column(
        Integer,
        ForeignKey('step.step_id', ondelete='CASCADE'),
        nullable=False
    )
    date_step_begin = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False
    )
    date_step_end: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True
    )


class Buy_Book(BaseModel):
    __tablename__ = 'buy_book'

    buy_book_id = Column(Integer, primary_key=True)
    buy_id = Column(
        Integer,
        ForeignKey('buy.buy_id', ondelete='CASCADE'),
        nullable=False
    )
    book_id = Column(
        Integer,
        ForeignKey('book.book_id', ondelete='CASCADE'),
        nullable=False
    )
    amount = Column(Integer, nullable=False, default=0)


BaseModel.metadata.create_all(engine)
