pacman::p_load(tidyverse)

targets::tar_load_everything()

the_plan = readRDS("RDS/the_plan.Rds")

results_objs = ls(pattern = "^results")

results_raw = apply(the_plan, 1, function(row) {
  target_obj_duckDB = results_objs[str_which(results_objs, paste0("results_", "duckDB_", row['source'], "_", row['csv_id'], "_", row['iteration']))] |> get()
  target_obj_read_csv = results_objs[str_which(results_objs, paste0("results_", "read_csv_", row['source'], "_", row['csv_id'], "_", row['iteration']))] |> get()

  tibble_row(
    read_csv_read_time = target_obj_read_csv$read_time,
    read_csv_query_dplyr_time = target_obj_read_csv$query_time[[1]],
    read_csv_query_duckplyr_time = target_obj_read_csv$query_time[[2]],
    duckDB_success = target_obj_duckDB$success,
    duckDB_read_time = target_obj_duckDB$read_time,
    duckDB_query_time = target_obj_duckDB$query_time
    )
}) |>
  bind_rows()

results = bind_cols(the_plan, results_raw)


results



# results_by_source_and_csv = results |>
#   group_by(source, csv_id) |>
#   summarise(
#     read_csv_read_time = mean(read_csv_read_time),
#     duckDB_read_time = mean(duckDB_read_time)
#   ) |>
#   ungroup() |>
#   arrange(csv_id, source)
#
# results_by_source_and_csv
#

#
# read_csv_LAN = filter(results_by_source_and_csv, source == 'LAN')$read_csv_read_time |> as.double()
# read_csv_OneDrive = filter(results_by_source_and_csv, source == 'OneDrive')$read_csv_read_time |> as.double()
#
# t.test(read_csv_LAN, read_csv_OneDrive, paired = T)
# wilcox.test(read_csv_LAN, read_csv_OneDrive, paired = T)





# let's check if LAN/OneDrive matters for load times.

results |>
  pivot_longer(cols = c('read_csv_read_time', 'duckDB_read_time')) |>
  ggplot(aes(x=csv_id, y=value, color=source)) +
  facet_wrap(~name) +
  geom_jitter() +
  geom_smooth(se=F)

# Visual inspection of this data makes it clear that OneDrive is way faster than LAN



# let's check if read_csv is faster than duckDB. We'll look at boxplots, both overall and by LAN/Onedrive source.

results |>
  pivot_longer(cols = c('read_csv_read_time', 'duckDB_read_time')) |>
  ggplot(aes(x=name, y=value)) +
  facet_wrap(~csv_id) +
  geom_boxplot()


results |>
  pivot_longer(cols = c('read_csv_read_time', 'duckDB_read_time')) |>
  ggplot(aes(x=name, y=value, color=source)) +
  facet_wrap(~csv_id) +
  geom_boxplot()

# Slightly lower times for read_csv, but it's tough to say overall.



# Let's look at query times

results |>
  select(csv_id, read_csv_query_dplyr_time, read_csv_query_duckplyr_time, duckDB_query_time) |>
  pivot_longer(cols = 2:4) |>
  ggplot(aes(x=name, y=value, color=name)) +
  facet_wrap(~csv_id) +
  geom_jitter(size=3, alpha=.5)
