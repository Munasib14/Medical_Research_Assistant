# run_retrieval.py
import sys
import traceback
from app.services.Retrieval_Generation_Service import RetrievalGenerationService
from app.core.logger import get_logger

logger = get_logger("RunRetrieval")


def retrieval():
    """
    CLI interface for running the RetrievalGenerationService.
    Accepts user query via command line argument or manual input.
    """
    try:
        logger.info("Starting RetrievalGenerationService CLI...")

        # Initialize retrieval service
        retrieval_service = RetrievalGenerationService()
        logger.info("RetrievalGenerationService initialized successfully.")

        # Accept query from CLI argument or prompt input
        if len(sys.argv) > 1:
            query = " ".join(sys.argv[1:])
            logger.info(f"Query received via CLI argument: {query}")
        else:
            query = input("\nEnter your query: ").strip()
            logger.info(f"Query received via user input: {query}")

        if not query:
            logger.warning("No query provided. Exiting.")
            print("No query provided. Exiting.")
            return

        logger.info(f"Processing query: {query}")

        # Get answer from retrieval service
        response = retrieval_service.get_answer(query)

        # Display results
        print("\n====================== Query Result ======================")
        print(f"Answer: {response.get('answer', 'No answer found.')}\n")

        sources = response.get("sources", [])

        if sources:
            print("=================== Source Documents ===================")
            for idx, doc in enumerate(sources, 1):
                print(f"\nSource {idx}:")
                print(f"Metadata: {doc.get('metadata')}")
                print(f"Content: {doc.get('content')[:500]}...")  # Display first 500 characters
        else:
            print("No source documents found for this query.")

        print("\n======================= End =======================")

    except Exception as e:
        logger.error(f"Error during retrieval: {e}")
        logger.error(traceback.format_exc())
        print(f"\nAn error occurred: {e}\nPlease check logs for details.")


if __name__ == "__main__":
    retrieval()
