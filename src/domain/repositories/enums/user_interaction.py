from enum import Enum

class UserActivityAction(Enum):
    LOGIN = 'LOGIN'
    VIEW_PRODUCT = 'VIEW_PRODUCT'
    PURCHASE = 'PURCHASE'
    LOGOUT = 'LOGOUT'
    ADD_TO_CART = 'ADD_TO_CART'
    REMOVE_FROM_CART = 'REMOVE_FROM_CART'
    SEARCH = 'SEARCH'
    OTHER = 'OTHER'
