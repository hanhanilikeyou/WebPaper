from WebTextCleaner.cleaning import AcademicContentFilter
from WebTextCleaner.extraction import extract_main_text
from WebTextCleaner.fasttext_filter import FastTextFilter
from WebTextCleaner.scibert_filter import SciBERTScorer
from WebTextCleaner.deduplication import deduplicate_texts

def process_html(html, fasttext_model_path):
    # Stage 1: Clean HTML
    text = AcademicContentFilter()

    # Stage 2: Extract main content
    main_paragraphs = extract_main_text(text)

    # Stage 3: FastText filter
    ft_filter = FastTextFilter(fasttext_model_path)
    filtered = ft_filter.filter_paragraphs(main_paragraphs)

    # Stage 4: SciBERT scoring
    scorer = SciBERTScorer()
    refined = scorer.score_paragraphs(filtered)

    # Stage 5: Deduplication
    unique = deduplicate_texts(refined)

    return unique
