insert into configuration(key, value)
values (:key, :value)
on conflict(key) do update set value = excluded.value
