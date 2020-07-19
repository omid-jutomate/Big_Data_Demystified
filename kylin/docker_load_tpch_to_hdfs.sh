echo "docker name is :"+$0

sudo docker cp  /home/kyline/tpch-dbgen/lineitem.tbl  $0:/home/admin
sudo docker cp  /home/kylin/tpch-dbgen/nation.tbl  $0:/home/admin
sudo docker cp  /home/kylin/tpch-dbgen/region.tbl  $0:/home/admin
sudo docker cp  /home/kylin/tpch-dbgen/part.tbl  $0:/home/admin
sudo docker cp  /home/kylin/tpch-dbgen/supplier.tbl  $0:/home/admin
sudo docker cp  /home/kylin/tpch-dbgen/customer.tbl  $0:/home/admin
sudo docker cp  /home/kylin/tpch-dbgen/orders.tbl  $0:/home/admin

hadoop fs -mkdir /user/hive/warehouse/tpch/
hadoop fs -mkdir /user/hive/warehouse/tpch/lineitem/
hadoop fs -mkdir /user/hive/warehouse/tpch/nation/
hadoop fs -mkdir /user/hive/warehouse/tpch/region/
hadoop fs -mkdir /user/hive/warehouse/tpch/part/
hadoop fs -mkdir /user/hive/warehouse/tpch/supplier/
hadoop fs -mkdir /user/hive/warehouse/tpch/customer/
hadoop fs -mkdir /user/hive/warehouse/tpch/orders/