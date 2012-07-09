{application,erloci,
             [{description,"Erlang OCI"},
              {vsn,"0.1.0"},
              {registered,[]},
              {applications,[kernel,stdlib]},
              {mod,{oci_app,[]}},
              {env,[]},
              {modules,[oci_app,oci_port,oci_port_empty_table_mock,
                        oci_port_mock,oci_session,oci_session_pool]}]}.
