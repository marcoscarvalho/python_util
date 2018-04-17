
#!c:\Python27\python.exe -u

# coding: utf-8

from Skype4Py import Skype
import sys

client = Skype()
client.Attach()
user = sys.argv[1]
message = ' '.join(sys.argv[2:]
print user
print message
#client.SendMessage(user, message)