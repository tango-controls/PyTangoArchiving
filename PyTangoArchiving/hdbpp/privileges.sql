create database $DBNAME;
grant all privileges on $DBNAME.* to 'manager'@'localhost' identified by '$MPASS';
grant all privileges on $DBNAME.* to 'manager'@'%' identified by '$MPASS';
grant all privileges on $DBNAME.* to 'browser'@'%' identified by '$BPASS';
grant all privileges on $DBNAME.* to 'browser'@'localhost' identified by '$BPASS';

