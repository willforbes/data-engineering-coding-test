import luigi
from pipeline.tasks.task_ge_check import DataCheckTask


def test_GE_check():
    """Test the DataCheckTask"""
    print("Testing DataCheckTask...")

    task = DataCheckTask()

    # Check if task can be built
    try:
        luigi.build([task], local_scheduler=True, workers=1)
        print("✅ DataCheckTask completed successfully")

        # Check if output exists
        if task.output().exists():
            print("✅ DataCheckTask output exists")
            return True
        else:
            print("❌ DataCheckTask output not found")
            return False

    except Exception as e:
        print(f"❌ DataCheckTask failed: {str(e)}")
        return False


if __name__ == "__main__":
    test_GE_check()
