import cx_Oracle
import sys

def printf (format, *args):
	sys.stdout.write (format % args)

def printException (exception):
	error, = exception.args
	printf ("Error code = %s\n",error.code);
	printf ("Error message = %s\n",error.message);
  
try:
	conn_str = 'service_inventory_owner/service_inventory_owner@(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = 10.41.11.95)(PORT = 1521))(CONNECT_DATA = (SID = pcon1)))'
	conn = cx_Oracle.connect(conn_str)
	print(conn.version)
except Exception as e:
    printf ('Failed to connect to %s\n',databaseName)
    printException (e)
    exit (1)

c = conn.cursor()

try:
	sql = '''select *
			  from service_inventory_owner.gvt_inv_designator d
			  where d.designator_value = /'CTA-30CQSM8K-013/'
'''
	#c.execute(sql, ('CTA-30CQSM8K-013'))
	c.execute(sql)
except Exception as e:
	printf ('Nao funfou\n')
	printException (e)
	exit (1)

count = cursor.fetchone ()[0]
printf ('Count = %d\n',count)

c.close ()
conn.close ()
exit (0)