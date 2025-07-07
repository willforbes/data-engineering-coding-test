import luigi
from pipeline.tasks.task_import import DataImportTask


def test_data_import_task():
    """Test the DataImportTask"""
    print("Testing DataImportTask...")

    task = DataImportTask()

    # Check if task can be built
    try:
        luigi.build([task], local_scheduler=True, workers=1)
        print("✅ DataImportTask completed successfully")

        # Check if output exists
        if task.output().exists():
            print("✅ DataImportTask output exists")
            return True
        else:
            print("❌ DataImportTask output not found")
            return False

    except Exception as e:
        print(f"❌ DataImportTask failed: {str(e)}")
        return False


if __name__ == "__main__":
    test_data_import_task()
