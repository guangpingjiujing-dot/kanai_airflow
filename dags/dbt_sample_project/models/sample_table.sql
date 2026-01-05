SELECT
  enrollment_id,
  lesson_id,
  scheduled_at,
  ROW_NUMBER() OVER (
    PARTITION BY enrollment_id
    ORDER BY scheduled_at, lesson_id
  ) AS lesson_seq
FROM [taitechWarehouseTraning].[learning_management_system].[lessons]
ORDER BY enrollment_id, lesson_seq;