INSERT INTO service_request.servicerequest_activities
(service_request_id, activities_id)
(select distinct sr_id, id from service_request.activity where sr_id is not null);