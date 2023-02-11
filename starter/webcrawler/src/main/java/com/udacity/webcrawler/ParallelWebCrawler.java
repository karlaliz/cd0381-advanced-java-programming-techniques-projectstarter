package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private  final int maxDepth;
  private final List<Pattern> ignoredUrls;
  private final ForkJoinPool pool;

  private final PageParserFactory parserFactory;

  @Inject
  ParallelWebCrawler(
          Clock clock,
          @Timeout Duration timeout,
          @PopularWordCount int popularWordCount,
          @TargetParallelism int threadCount,
          @MaxDepth int maxDepth,
          @IgnoredUrls List<Pattern> ignoredUrls,
          PageParserFactory parserFactory) {
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.maxDepth = maxDepth;
    this.ignoredUrls = ignoredUrls;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.parserFactory = parserFactory;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {

    Instant deadline = clock.instant().plus(timeout);
    Set<String> visitedUrls = Collections.synchronizedSet(new HashSet<String>());

    if (maxDepth == 0 ) {
      return new CrawlResult.Builder()
              .setWordCounts(Collections.emptyMap())
              .setUrlsVisited(visitedUrls.size())
              .build();
    }

    Map<String, Integer> counts = crawlInternal(startingUrls, deadline, maxDepth, visitedUrls);

    if (counts.isEmpty()) {
      return new CrawlResult.Builder()
              .setWordCounts(counts)
              .setUrlsVisited(visitedUrls.size())
              .build();
    }

    return new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(counts, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build();

  }

  public class ForkJoinCrawl extends RecursiveTask< Map<String, Integer> > {

    private String url;
    private Instant deadline;
    private int maxDepth;
    private Set<String> visitedUrls;


    public ForkJoinCrawl(String url, Instant deadline, int maxDepth, Set<String> visitedUrls) {
      this.url = url;
      this.deadline = deadline;
      this.maxDepth = maxDepth;
      this.visitedUrls = visitedUrls;
    }

    @Override
    protected Map<String, Integer> compute() {
      if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
        return Collections.emptyMap();
      }

      if (visitedUrls.contains(url)) {
        return Collections.emptyMap();
      }
      visitedUrls.add(url);
      PageParser.Result result = parserFactory.get(url).parse();

      Map<String, Integer>  counts = crawlInternal(result.getLinks(),  deadline, maxDepth - 1, visitedUrls);
      Map<String, Integer>  wordCounts = new HashMap<>();
      addToCounts(wordCounts, counts);
      addToCounts(wordCounts, result.getWordCounts());
      return wordCounts;
    }

  }

  private boolean ignoreUrl(String url) {
    for (Pattern pattern : ignoredUrls) {
      if (pattern.matcher(url).matches()) {
        return true;
      }
    }
    return  false;
  }

  private Map<String, Integer> crawlInternal(
          List<String> links,
          Instant deadline,
          int maxDepth,
          Set<String> visitedUrls)
  {
    if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
      return Collections.emptyMap();
    }

    List<ForkJoinCrawl> crawlList = new ArrayList<>();
    for (String link : links) {
      if (!ignoreUrl(link)) {
        ForkJoinCrawl crawl = new ForkJoinCrawl(link, deadline, maxDepth , visitedUrls);
        crawlList.add(crawl);
      }
    }

    Map<String, Integer> counts = new HashMap<>();
    Collection<ForkJoinCrawl> results = ForkJoinTask.invokeAll(crawlList);
    for(ForkJoinCrawl crawl : results) {
      addToCounts(counts, crawl.getRawResult());
    }

    return counts;
  }

  private static void addToCounts(Map<String, Integer> counts, Map<String, Integer> counts2) {
    for (Map.Entry<String, Integer> e : counts2.entrySet()) {
      if (counts.containsKey(e.getKey())) {
        counts.put(e.getKey(), e.getValue() + counts.get(e.getKey()));
      } else {
        counts.put(e.getKey(), e.getValue());
      }
    }
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}
