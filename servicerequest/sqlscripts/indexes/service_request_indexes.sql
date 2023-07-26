CREATE INDEX stage_voucher_detail_id ON ng_intermediate_data_store.stage_service_request USING btree (voucher_header_id);

CREATE INDEX stage_cb_evnt_bkng_sysid ON ng_intermediate_data_store.stage_service_request USING btree (cb_evnt_bkng_sysid);

CREATE INDEX onsale_reg_evt_bkng_id ON ng_intermediate_data_store.stage_service_request USING btree (onsale_reg_evt_bkng_id);

CREATE INDEX stage_member_id ON ng_intermediate_data_store.stage_service_request USING btree (member_id);

CREATE INDEX stage_membership_id ON ng_intermediate_data_store.stage_service_request USING btree (membership_id);

ALTER TABLE service_request.activity ADD sr_id bigint null;