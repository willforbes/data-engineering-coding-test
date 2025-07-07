import luigi
import os
from pipeline.tasks.task_genre_split import GenreSplitCoordinatorTask


def test_genre_split_task():
    """Test the GenreSplitCoordinatorTask"""
    print("Testing GenreSplitCoordinatorTask...")

    task = GenreSplitCoordinatorTask()

    # Check if task can be built
    try:
        luigi.build([task], local_scheduler=True, workers=1)
        print("✅ GenreSplitCoordinatorTask completed successfully")

        # Check if output exists
        if task.output().exists():
            print("✅ GenreSplitCoordinatorTask output exists")

            # Check if genre files were created
            genres_dir = os.path.join("output", "genres")
            if os.path.exists(genres_dir):
                genre_files = [
                    f for f in os.listdir(genres_dir) if f.endswith(".parquet")
                ]
                print(f"✅ Found {len(genre_files)} genre parquet files")
                return True
            else:
                print("❌ Genres directory not found")
                return False
        else:
            print("❌ GenreSplitCoordinatorTask output not found")
            return False

    except Exception as e:
        print(f"❌ GenreSplitCoordinatorTask failed: {str(e)}")
        return False


if __name__ == "__main__":
    test_genre_split_task()
