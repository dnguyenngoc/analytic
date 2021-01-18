#!/bin/sh

echo -e "remove old migration....\n"
rm -r migrations/alembic/version*

echo -e "remove old migration session db....\n"
python3 migrations/restore.py

echo -e "create migration....\n"
alembic -c migrations/alembic.ini revision --autogenerate -m 'Auto migration' 

echo -e "execute migration....\n"
alembic -c migrations/alembic.ini upgrade head && 
echo -e "Done!"
