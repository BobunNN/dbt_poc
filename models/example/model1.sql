{{ config(materialized='table') }}


SELECT * FROM `souscritoo-1343.save_nhan_DE.sent_audios_france_copy`
WHERE `dur_comm_call` LIKE "%1)%"