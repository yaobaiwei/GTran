# README

1. Replace your gquery-conf.ini, machine.cfg and ib_conf with given one.

2. Start the server on worker21. [Double check there is no other programs running on each machines]

   `bash> bash start-server.sh 4 machine.cfg ib_conf`

3. Start a client on worker24

   `bash> bash start-client.sh ib_conf`

4. Build Index in console

   `GQuery> gquery -b ldbc_index_build`

5. Run queries sequentially in console and check the result

   `GQeury> gquery -q g.V().hasLabel("comment").count()`

   `GQeury> gquery -q g.V().has("firstName", "Yang").count() `

   `GQeury> gquery -q g.V().has("ori_id", "4947804876981").properties() `

   `GQeury> gquery -q A = g.addV("person"); g.V().has("firstName", "Aaron").limit(1).addE("knows").to(A) `

   `GQeury> gquery -q g.V().has("firstName", "Jack").drop()`

   `GQuery> gquery -q g.V().count()`

6. Run three throughput tests in console.

   `GQuery> gquery -q emu thpt_config1 microbench_ldbc`

   Result: 19.8779 Kqueries/sec

   `GQuery> gquery -q emu thpt_config2 microbench_ldbc`

   Result: 15.8851 Kqueries/sec

   `GQuery> gquery -q emu thpt_config3 microbench_ldbc`

   Result: 7.0241 Kqueries/sec

   Three `thpt_config` represents three workloads: ReadOnly, ReadIntensive and WriteIntensive