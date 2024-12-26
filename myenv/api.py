from fastapi import FastAPI, HTTPException, Depends, status, Header
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import List, Dict
from datetime import datetime
import sqlalchemy
from typing import Optional
import os
from passlib.context import CryptContext
from datetime import timedelta
from jose import jwt

import dal

app = FastAPI ()
oauth2_scheme = OAuth2PasswordBearer ( tokenUrl="token" )

# Pydantic models
class Token ( BaseModel ):
    access_token: str
    token_type: str


class UserCreate ( BaseModel ):
    username: str
    password: str
    email: str
    is_admin: bool = False


class UserRead ( BaseModel ):
    id: int
    username: str
    email: str
    is_active: bool
    is_admin: bool


class MailingListCreate ( BaseModel ):
    name: str


class MailingListRead ( BaseModel ):
    id: int
    name: str
    user_id: int


class SubscriberCreate ( BaseModel ):
    email: str
    mailing_list_id: int


class SubscriberRead ( BaseModel ):
    id: int
    email: str
    mailing_list_id: int


class TemplateCreate ( BaseModel ):
    name: str
    content: str


class TemplateRead ( BaseModel ):
    id: int
    name: str
    content: str
    user_id: int


class MailingCreate ( BaseModel ):
    mailing_list_id: int
    template_id: int
    scheduled_at: datetime


class MailingRead ( BaseModel ):
    id: int
    mailing_list_id: int
    template_id: int
    scheduled_at: datetime
    sent_at: Optional[datetime] = None
    user_id: int


# Dependency для получения текущего пользователя
async def get_current_user(token: str = Depends ( oauth2_scheme ),
                           user_dao: dal.UserDAO = Depends ( dal.DAOFactory.get_user_dao )) -> dal.User:
    payload = verify_token ( token )
    if payload is None:
        raise HTTPException ( status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token" )
    user_id = payload.get ( "user_id" )
    if user_id is None:
        raise HTTPException ( status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token" )

    user = await user_dao.get_user_by_id ( user_id )
    if user is None:
        raise HTTPException ( status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid user" )
    return user


# Dependency для получения текущего админа
async def get_current_admin(current_user: dal.User = Depends ( get_current_user )) -> dal.User:
    if not current_user.is_admin:
        raise HTTPException ( status_code=status.HTTP_403_FORBIDDEN, detail="You must be an admin" )
    return current_user


# Dependency для доступа к UserDAO
async def get_user_dao() -> dal.UserDAO:
    return dal.DAOFactory.get_user_dao()


# Dependency для доступа к MailingListDAO
async def get_mailing_list_dao() -> dal.MailingListDAO:
    return dal.DAOFactory.get_mailing_list_dao ()


# Dependency для доступа к SubscriberDAO
async def get_subscriber_dao() -> dal.SubscriberDAO:
    return dal.DAOFactory.get_subscriber_dao ()


# Dependency для доступа к TemplateDAO
async def get_template_dao() -> dal.TemplateDAO:
    return dal.DAOFactory.get_template_dao ()


# Dependency для доступа к MailingDAO
async def get_mailing_dao() -> dal.MailingDAO:
    return dal.DAOFactory.get_mailing_dao ()


pwd_context = CryptContext ( schemes=["bcrypt"], deprecated="auto" )


def get_hashed_password(password: str) -> str:
    return pwd_context.hash ( password )


def verify_password(password: str, hashed_password: str) -> bool:
    return pwd_context.verify ( password, hashed_password )


SECRET_KEY = os.getenv ( "SECRET_KEY" )
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


def create_access_token(data: dict, expires_delta: timedelta = timedelta ( minutes=ACCESS_TOKEN_EXPIRE_MINUTES )):
    to_encode = data.copy ()
    expire = datetime.utcnow () + expires_delta
    to_encode.update ( {"exp": expire} )
    encoded_jwt = jwt.encode ( to_encode, SECRET_KEY, algorithm=ALGORITHM )
    return encoded_jwt


def verify_token(token: str):
    try:
        payload = jwt.decode ( token, SECRET_KEY, algorithms=[ALGORITHM] )
        return payload
    except Exception:
        return None


# Аутентификация
@app.post ( "/token", response_model=Token )
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends (),
                                 user_dao: dal.UserDAO = Depends ( get_user_dao )):
    user = await user_dao.get_user_by_username ( form_data.username )
    if user is None or not verify_password ( form_data.password, user.hashed_password ):
        raise HTTPException ( status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password" )
    access_token = create_access_token ( data={"user_id": user.id} )
    return {"access_token": access_token, "token_type": "bearer"}


# CRUD для пользователей
@app.post ( "/users", response_model=UserRead, status_code=status.HTTP_201_CREATED )
async def create_new_user(user: UserCreate, user_dao: dal.UserDAO = Depends ( get_user_dao )):
    hashed_password = get_hashed_password ( user.password )
    return await user_dao.create_user ( username=user.username, hashed_password=hashed_password, email=user.email,
                                        is_admin=user.is_admin )


@app.get ( "/users/me", response_model=UserRead )
async def read_current_user(current_user: dal.User = Depends ( get_current_user )):
    return current_user


@app.put ( "/users/{user_id}", response_model=UserRead )
async def update_user(user_id: int, user: UserCreate, current_admin: dal.User = Depends ( get_current_admin ),
                      user_dao: dal.UserDAO = Depends ( get_user_dao )):
    hashed_password = get_hashed_password ( user.password )
    update_user = await user_dao.update_user ( user_id=user_id, username=user.username, hashed_password=hashed_password,
                                               email=user.email, is_admin=user.is_admin )
    if not update_user:
        raise HTTPException ( status_code=404, detail="User not found" )
    return update_user


@app.get ( "/users", response_model=List[UserRead] )
async def read_all_users(current_admin: dal.User = Depends ( get_current_admin ),
                         user_dao: dal.UserDAO = Depends ( get_user_dao )):
    return await user_dao.get_all_users ()


@app.delete ( "/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT )
async def delete_user(user_id: int, current_admin: dal.User = Depends ( get_current_admin ),
                      user_dao: dal.UserDAO = Depends ( get_user_dao )):
    if not await user_dao.delete_user ( user_id ):
        raise HTTPException ( status_code=404, detail="User not found" )


# CRUD для списка рассылки
@app.post ( "/mailing_lists", response_model=MailingListRead, status_code=status.HTTP_201_CREATED )
async def create_new_mailing_list(mailing_list: MailingListCreate, current_user: dal.User = Depends ( get_current_user ),
                                  mailing_list_dao: dal.MailingListDAO = Depends ( get_mailing_list_dao )):
    return await mailing_list_dao.create_mailing_list ( name=mailing_list.name, user_id=current_user.id )


@app.get ( "/mailing_lists", response_model=List[MailingListRead] )
async def read_all_mailing_lists(current_user: dal.User = Depends ( get_current_user ),
                                 mailing_list_dao: dal.MailingListDAO = Depends ( get_mailing_list_dao )):
    return await mailing_list_dao.get_mailing_lists_by_user_id ( user_id=current_user.id )


@app.get ( "/mailing_lists/{mailing_list_id}", response_model=MailingListRead )
async def read_mailing_list(mailing_list_id: int, current_user: dal.User = Depends ( get_current_user ),
                            mailing_list_dao: dal.MailingListDAO = Depends ( get_mailing_list_dao )):
    mailing_list = await mailing_list_dao.get_mailing_list_by_id ( mailing_list_id )
    if not mailing_list or mailing_list.user_id != current_user.id:
        raise HTTPException ( status_code=404, detail="Mailing list not found" )
    return mailing_list


@app.put ( "/mailing_lists/{mailing_list_id}", response_model=MailingListRead )
async def update_mailing_list(mailing_list_id: int, mailing_list: MailingListCreate,
                              current_user: dal.User = Depends ( get_current_user ),
                              mailing_list_dao: dal.MailingListDAO = Depends ( get_mailing_list_dao )):
    mailing_list_update = await mailing_list_dao.update_mailing_list ( mailing_list_id=mailing_list_id,
                                                                       name=mailing_list.name )
    if not mailing_list_update or mailing_list_update.user_id != current_user.id:
        raise HTTPException ( status_code=404, detail="Mailing list not found" )
    return mailing_list_update


@app.delete ( "/mailing_lists/{mailing_list_id}", status_code=status.HTTP_204_NO_CONTENT )
async def delete_mailing_list(mailing_list_id: int, current_user: dal.User = Depends ( get_current_user ),
                              mailing_list_dao: dal.MailingListDAO = Depends ( get_mailing_list_dao )):
    mailing_list = await mailing_list_dao.get_mailing_list_by_id ( mailing_list_id )
    if not mailing_list or mailing_list.user_id != current_user.id:
        raise HTTPException ( status_code=404, detail="Mailing list not found" )
    if not await mailing_list_dao.delete_mailing_list ( mailing_list_id ):
        raise HTTPException ( status_code=404, detail="Mailing list not found" )


# CRUD для подписчиков
@app.post ( "/subscribers", response_model=SubscriberRead, status_code=status.HTTP_201_CREATED )
async def create_new_subscriber(subscriber: SubscriberCreate, current_user: dal.User = Depends ( get_current_user ),
                                mailing_list_dao: dal.MailingListDAO = Depends ( get_mailing_list_dao ),
                                subscriber_dao: dal.SubscriberDAO = Depends ( get_subscriber_dao )):
    mailing_list = await mailing_list_dao.get_mailing_list_by_id ( subscriber.mailing_list_id )
    if not mailing_list or mailing_list.user_id != current_user.id:
        raise HTTPException ( status_code=404, detail="Mailing list not found" )
    return await subscriber_dao.create_subscriber ( email=subscriber.email, mailing_list_id=subscriber.mailing_list_id )


@app.get ( "/subscribers/{mailing_list_id}", response_model=List[SubscriberRead] )
async def read_all_subscribers(mailing_list_id: int, current_user: dal.User = Depends ( get_current_user ),
                               mailing_list_dao: dal.MailingListDAO = Depends ( get_mailing_list_dao ),
                               subscriber_dao: dal.SubscriberDAO = Depends ( get_subscriber_dao )):
    mailing_list = await mailing_list_dao.get_mailing_list_by_id ( mailing_list_id )
    if not mailing_list or mailing_list.user_id != current_user.id:
        raise HTTPException ( status_code=404, detail="Mailing list not found" )
    return await subscriber_dao.get_subscribers_by_mailing_list_id ( mailing_list_id=mailing_list_id )


@app.delete ( "/subscribers/{subscriber_id}", status_code=status.HTTP_204_NO_CONTENT )
async def delete_subscriber(subscriber_id: int, current_user: dal.User = Depends ( get_current_user ),
                            subscriber_dao: dal.SubscriberDAO = Depends ( get_subscriber_dao ),
                            mailing_list_dao: dal.MailingListDAO = Depends ( get_mailing_list_dao )):
    subscriber = await subscriber_dao.get_subscriber_by_id ( subscriber_id )
    if not subscriber:
        raise HTTPException ( status_code=404, detail="Subscriber not found" )

    mailing_list = await mailing_list_dao.get_mailing_list_by_id ( subscriber.mailing_list_id )
    if not mailing_list or mailing_list.user_id != current_user.id:
        raise HTTPException ( status_code=404, detail="Mailing list not found" )
    if not await subscriber_dao.delete_subscriber ( subscriber_id ):
        raise HTTPException ( status_code=404, detail="Subscriber not found" )


# CRUD для шаблонов
@app.post ( "/templates", response_model=TemplateRead, status_code=status.HTTP_201_CREATED )
async def create_new_template(template: TemplateCreate, current_user: dal.User = Depends ( get_current_user ),
                              template_dao: dal.TemplateDAO = Depends ( get_template_dao )):
    return await template_dao.create_template ( name=template.name, content=template.content, user_id=current_user.id )


@app.get ( "/templates", response_model=List[TemplateRead] )
async def read_all_templates(current_user: dal.User = Depends ( get_current_user ),
                             template_dao: dal.TemplateDAO = Depends ( get_template_dao )):
    return await template_dao.get_templates_by_user_id ( user_id=current_user.id )


@app.get ( "/templates/{template_id}", response_model=TemplateRead )
async def read_template(template_id: int, current_user: dal.User = Depends ( get_current_user ),
                        template_dao: dal.TemplateDAO = Depends ( get_template_dao )):
    template = await template_dao.get_template_by_id ( template_id )
    if not template or template.user_id != current_user.id:
        raise HTTPException ( status_code=404, detail="Template not found" )
    return template


@app.put ( "/templates/{template_id}", response_model=TemplateRead )
async def update_template(template_id: int, template: TemplateCreate,
                          current_user: dal.User = Depends ( get_current_user ),
                          template_dao: dal.TemplateDAO = Depends ( get_template_dao )):
    template_update = await template_dao.update_template ( template_id=template_id, name=template.name,
                                                           content=template.content )
    if not template_update or template_update.user_id != current_user.id:
        raise HTTPException ( status_code=404, detail="Template not found" )
    return template_update


@app.delete ( "/templates/{template_id}", status_code=status.HTTP_204_NO_CONTENT )
async def delete_template(template_id: int, current_user: dal.User = Depends ( get_current_user ),
                          template_dao: dal.TemplateDAO = Depends ( get_template_dao )):
    template = await template_dao.get_template_by_id ( template_id )
    if not template:
        raise HTTPException ( status_code=404, detail="Template not found" )
    if template.user_id != current_user.id:
        raise HTTPException ( status_code=403, detail="You are not allowed to delete this template" )
    if not await template_dao.delete_template ( template_id ):
        raise HTTPException ( status_code=404, detail="Template not found" )
