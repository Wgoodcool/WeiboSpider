# -*- coding: utf-8 -*-

from multiprocessing.managers import BaseManager
from database import UserOpe, MblogOpe, FanOpe, FolOpe, proxyOpe

class InfoManager (BaseManager): 
    pass

InfoManager.register('UserOpe', UserOpe)
InfoManager.register('MblogOpe', MblogOpe)
InfoManager.register('FanOpe', FanOpe)
InfoManager.register('FolOpe', FolOpe)
InfoManager.register('proxyOpe', proxyOpe)

