import os
from typing import List, Optional
from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from dotenv import load_dotenv
from datetime import datetime
import sqlalchemy
load_dotenv ('my_db.env')

# Базовый класс для моделей SQLAlchemy
Base = declarative_base ()


# Определение моделей базы данных
class User ( Base ):
    __tablename__ = "users"

    id = Column ( Integer, primary_key=True, index=True )
    username = Column ( String, unique=True, nullable=False )
    hashed_password = Column ( String, nullable=False )
    email = Column ( String, unique=True, nullable=False )
    is_active = Column ( Boolean, default=True )
    is_admin = Column ( Boolean, default=False )

    mailing_lists = relationship ( "MailingList", back_populates="user", cascade="all, delete-orphan" )
    templates = relationship ( "Template", back_populates="user", cascade="all, delete-orphan" )
    mailings = relationship ( "Mailing", back_populates="user", cascade="all, delete-orphan" )


class MailingList ( Base ):
    __tablename__ = "mailing_lists"
    id = Column ( Integer, primary_key=True, index=True )
    name = Column ( String, nullable=False )
    user_id = Column ( Integer, ForeignKey ( "users.id", ondelete="CASCADE" ) )

    user = relationship ( "User", back_populates="mailing_lists" )
    subscribers = relationship ( "Subscriber", back_populates="mailing_list", cascade="all, delete-orphan" )
    mailings = relationship ( "Mailing", back_populates="mailing_list", cascade="all, delete-orphan" )


class Subscriber ( Base ):
    __tablename__ = "subscribers"

    id = Column ( Integer, primary_key=True, index=True )
    email = Column ( String, unique=True, nullable=False )
    mailing_list_id = Column ( Integer, ForeignKey ( "mailing_lists.id", ondelete="CASCADE" ) )

    mailing_list = relationship ( "MailingList", back_populates="subscribers" )


class Template ( Base ):
    __tablename__ = "templates"

    id = Column ( Integer, primary_key=True, index=True )
    name = Column ( String, nullable=False )
    content = Column ( Text, nullable=False )
    user_id = Column ( Integer, ForeignKey ( "users.id", ondelete="CASCADE" ) )

    user = relationship ( "User", back_populates="templates" )
    mailings = relationship ( "Mailing", back_populates="template", cascade="all, delete-orphan" )


class Mailing ( Base ):
    __tablename__ = "mailings"
    id = Column ( Integer, primary_key=True, index=True )
    mailing_list_id = Column ( Integer, ForeignKey ( "mailing_lists.id", ondelete="NO ACTION" ) )
    template_id = Column ( Integer, ForeignKey ( "templates.id", ondelete="NO ACTION" ) )
    scheduled_at = Column ( DateTime, nullable=True )
    sent_at = Column ( DateTime, nullable=True )
    user_id = Column ( Integer, ForeignKey ( "users.id", ondelete="NO ACTION" ) )

    mailing_list = relationship ( "MailingList", back_populates="mailings" )
    template = relationship ( "Template", back_populates="mailings" )
    user = relationship ( "User", back_populates="mailings" )


# Паттерн "Жадный синглтон" для управления сессиями базы данных
class DatabaseSessionManager:
    _instance = None

    def __new__(cls, db_url: str = None):
        if cls._instance is None:
            cls._instance = super ( DatabaseSessionManager, cls ).__new__ ( cls )
            if db_url is None:
                db_url = os.getenv ( 'DATABASE_URL' )

            if db_url is None:
                raise ValueError ( 'Database URL is not set. Please provide DATABASE_URL environment variable.' )

            sync_engine = create_engine ( db_url )  # Создание sync_engine
            cls._instance.async_engine = create_async_engine ( db_url )
            cls._instance.SessionLocal = sessionmaker ( autocommit=False, autoflush=False, bind=sync_engine )
            Base.metadata.create_all ( bind=sync_engine )
        return cls._instance

    def get_sync_session(self):
        return self.SessionLocal ()

    async def get_async_session(self) -> AsyncSession:
        async_session = sessionmaker(self.async_engine, class_=AsyncSession, expire_on_commit=False)
        return async_session ()

    def close(self):
        if self._instance.async_engine:
            self._instance.async_engine.dispose ()
        self._instance = None


database_session_manager = DatabaseSessionManager ()  # Создание экземпляра синглтона


# Data access object для пользователей
class UserDAO:
    def __init__(self, session_manager: DatabaseSessionManager = database_session_manager):
        self.session_manager = session_manager

    async def create_user(self, username: str, hashed_password: str, email: str, is_admin: bool = False) -> User:
        async with await self.session_manager.get_async_session () as session:
            new_user = User ( username=username, hashed_password=hashed_password, email=email, is_admin=is_admin )
            session.add ( new_user )
            await session.commit ()
            await session.refresh ( new_user )
        return new_user

    async def get_user_by_username(self, username: str) -> Optional[User]:
        async with await self.session_manager.get_async_session () as session:
            return (await session.execute (
                sqlalchemy.select ( User ).filter_by ( username=username ) )).scalar_one_or_none ()

    async def get_user_by_email(self, email: str) -> Optional[User]:
        async with await self.session_manager.get_async_session () as session:
            return (
                await session.execute ( sqlalchemy.select ( User ).filter_by ( email=email ) )).scalar_one_or_none ()

    async def get_user_by_id(self, user_id: int) -> Optional[User]:
        async with await self.session_manager.get_async_session () as session:
            return (await session.execute ( sqlalchemy.select ( User ).filter_by ( id=user_id ) )).scalar_one_or_none ()

    async def update_user(self, user_id: int, username: str = None, hashed_password: str = None, email: str = None,
                          is_admin: bool = None) -> Optional[User]:
        async with await self.session_manager.get_async_session () as session:
            user = await self.get_user_by_id ( user_id )
            if user:
                if username:
                    user.username = username
                if hashed_password:
                    user.hashed_password = hashed_password
                if email:
                    user.email = email
                if is_admin is not None:
                    user.is_admin = is_admin
                await session.commit ()
                return user
            return None

    async def get_all_users(self) -> List[User]:
        async with await self.session_manager.get_async_session () as session:
            result = await session.execute ( sqlalchemy.select ( User ) )
            return list ( result.scalars ().all () )

    async def delete_user(self, user_id: int) -> bool:
        async with await self.session_manager.get_async_session () as session:
            user = await self.get_user_by_id ( user_id )
            if user:
                await session.delete ( user )
                await session.commit ()
                return True
            return False


# Data access object для списка рассылки
class MailingListDAO:

    def __init__(self, session_manager: DatabaseSessionManager = database_session_manager):
        self.session_manager = session_manager

    async def create_mailing_list(self, name: str, user_id: int) -> MailingList:
        async with await self.session_manager.get_async_session () as session:
            new_mailing_list = MailingList ( name=name, user_id=user_id )
            session.add ( new_mailing_list )
            await session.commit ()
            await session.refresh ( new_mailing_list )
        return new_mailing_list

    async def get_mailing_list_by_id(self, mailing_list_id: int) -> Optional[MailingList]:
        async with await self.session_manager.get_async_session () as session:
            return (await session.execute (
                sqlalchemy.select ( MailingList ).filter_by ( id=mailing_list_id ) )).scalar_one_or_none ()

    async def get_mailing_lists_by_user_id(self, user_id: int) -> List[MailingList]:
        async with await self.session_manager.get_async_session () as session:
            result = await session.execute ( sqlalchemy.select ( MailingList ).filter_by ( user_id=user_id ) )
            return list ( result.scalars ().all () )

    async def update_mailing_list(self, mailing_list_id: int, name: str = None) -> Optional[MailingList]:
        async with await self.session_manager.get_async_session () as session:
            mailing_list = await self.get_mailing_list_by_id ( mailing_list_id )
            if mailing_list and name:
                mailing_list.name = name
                await session.commit ()
                return mailing_list
            return None

    async def delete_mailing_list(self, mailing_list_id: int) -> bool:
        async with await self.session_manager.get_async_session () as session:
            mailing_list = await self.get_mailing_list_by_id ( mailing_list_id )
            if mailing_list:
                await session.delete ( mailing_list )
                await session.commit ()
                return True
            return False


# Data access object для подписчиков
class SubscriberDAO:
    def __init__(self, session_manager: DatabaseSessionManager = database_session_manager):
        self.session_manager = session_manager

    async def create_subscriber(self, email: str, mailing_list_id: int) -> Subscriber:
        async with await self.session_manager.get_async_session () as session:
            new_subscriber = Subscriber ( email=email, mailing_list_id=mailing_list_id )
            session.add ( new_subscriber )
            await session.commit ()
            await session.refresh ( new_subscriber )
        return new_subscriber

    async def get_subscriber_by_email(self, email: str, mailing_list_id: int) -> Optional[Subscriber]:
        async with await self.session_manager.get_async_session () as session:
            return (await session.execute ( sqlalchemy.select ( Subscriber ).filter_by ( email=email,
                                                                                         mailing_list_id=mailing_list_id ) )).scalar_one_or_none ()

    async def get_subscribers_by_mailing_list_id(self, mailing_list_id: int) -> List[Subscriber]:
        async with await self.session_manager.get_async_session () as session:
            result = await session.execute (
                sqlalchemy.select ( Subscriber ).filter_by ( mailing_list_id=mailing_list_id ) )
            return list ( result.scalars ().all () )

    async def delete_subscriber(self, subscriber_id: int) -> bool:
        async with await self.session_manager.get_async_session () as session:
            subscriber = await self.get_subscriber_by_id ( subscriber_id )
            if subscriber:
                await session.delete ( subscriber )
                await session.commit ()
                return True
            return False

    async def get_subscriber_by_id(self, subscriber_id: int) -> Optional[Subscriber]:
        async with await self.session_manager.get_async_session () as session:
            return (await session.execute (
                sqlalchemy.select ( Subscriber ).filter_by ( id=subscriber_id ) )).scalar_one_or_none ()


# Data access object для шаблонов
class TemplateDAO:

    def __init__(self, session_manager: DatabaseSessionManager = database_session_manager):
        self.session_manager = session_manager

    async def create_template(self, name: str, content: str, user_id: int) -> Template:
        async with await self.session_manager.get_async_session () as session:
            new_template = Template ( name=name, content=content, user_id=user_id )
            session.add ( new_template )
            await session.commit ()
            await session.refresh ( new_template )
        return new_template

    async def get_template_by_id(self, template_id: int) -> Optional[Template]:
        async with await self.session_manager.get_async_session () as session:
            return (await session.execute (
                sqlalchemy.select ( Template ).filter_by ( id=template_id ) )).scalar_one_or_none ()

    async def get_templates_by_user_id(self, user_id: int) -> List[Template]:
        async with await self.session_manager.get_async_session () as session:
            result = await session.execute ( sqlalchemy.select ( Template ).filter_by ( user_id=user_id ) )
            return list ( result.scalars ().all () )

    async def update_template(self, template_id: int, name: str = None, content: str = None) -> Optional[Template]:
        async with await self.session_manager.get_async_session () as session:
            template = await self.get_template_by_id ( template_id )
            if template:
                if name:
                    template.name = name
                if content:
                    template.content = content
                await session.commit ()
                return template
            return None

    async def delete_template(self, template_id: int) -> bool:
        async with await self.session_manager.get_async_session () as session:
            template = await self.get_template_by_id ( template_id )
            if template:
                await session.delete ( template )
                await session.commit ()
                return True
            return False


# Data access object для рассылок
class MailingDAO:
    def __init__(self, session_manager: DatabaseSessionManager = database_session_manager):
        self.session_manager = session_manager

    async def create_mailing(self, mailing_list_id: int, template_id: int, scheduled_at: datetime,
                             user_id: int) -> Mailing:
        async with await self.session_manager.get_async_session () as session:
            new_mailing = Mailing ( mailing_list_id=mailing_list_id, template_id=template_id, scheduled_at=scheduled_at,
                                    user_id=user_id )
            session.add ( new_mailing )
            await session.commit ()
            await session.refresh ( new_mailing )
        return new_mailing

    async def get_mailing_by_id(self, mailing_id: int) -> Optional[Mailing]:
        async with await self.session_manager.get_async_session () as session:
            return (await session.execute (
                sqlalchemy.select ( Mailing ).filter_by ( id=mailing_id ) )).scalar_one_or_none ()

    async def update_mailing(self, mailing_id: int, sent_at: datetime) -> Optional[Mailing]:
        async with await self.session_manager.get_async_session () as session:
            mailing = await self.get_mailing_by_id ( mailing_id )
            if mailing:
                mailing.sent_at = sent_at
                await session.commit ()
                return mailing
            return None

    async def get_mailings_by_user_id(self, user_id: int) -> List[Mailing]:
        async with await self.session_manager.get_async_session () as session:
            result = await session.execute ( sqlalchemy.select ( Mailing ).filter_by ( user_id=user_id ) )
            return list ( result.scalars ().all () )

    async def get_all_mailings(self) -> List[Mailing]:
        async with await self.session_manager.get_async_session () as session:
            result = await session.execute ( sqlalchemy.select ( Mailing ) )
            return list ( result.scalars ().all () )


# Фабричный класс для создания Data Access Objects
class DAOFactory:
    @staticmethod
    def get_user_dao():
        return UserDAO ()

    @staticmethod
    def get_mailing_list_dao():
        return MailingListDAO ()

    @staticmethod
    def get_subscriber_dao():
        return SubscriberDAO ()

    @staticmethod
    def get_template_dao():
        return TemplateDAO ()

    @staticmethod
    def get_mailing_dao():
        return MailingDAO ()
