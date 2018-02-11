consistency QUORUM;
consistency;
----1) Get password by driver name, in order to check whether the old password is right.
select password from driver where driver_name = 'Jane';
