CREATE TABLE service_request.sr_member_mapping (
    id bigint NOT NULL,
    member jsonb
);

UPDATE service_request.servicerequest sr
SET member = s.member
FROM sr_member_mapping s
WHERE s.id = sr.id