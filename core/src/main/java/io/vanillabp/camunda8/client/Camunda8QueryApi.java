package io.vanillabp.camunda8.client;

import java.util.function.Supplier;

import io.camunda.client.CamundaClient;
import lombok.extern.slf4j.Slf4j;

/**
 * Whether the cluster of one adapter id answers query-API requests at all - asked once
 * while the adapter starts processing a workflow module, and remembered from then on.
 *
 * <h2>Why the answer is remembered rather than derived per failure</h2>
 *
 * Finding a workflow by its aggregate's ID is a search, and so is everything the viewer
 * and the version catalog ask; a cluster started without secondary storage refuses all of
 * them. What the adapter does about it differs per question - the election probe answers
 * optimistically, while the push of a changed aggregate fails with a guiding message - so
 * each of those places has to know WHY its request failed. Reading that out of the failure itself is what this class replaces: the cluster
 * refuses a query endpoint with the same HTTP 403 it uses for a request the credentials
 * may not make, and it says which of the two it was in prose only. So a failure is not
 * asked what it means; the capability is settled once, by a request whose only purpose is
 * to find out, and every later failure is read against that answer.
 *
 * <h2>What a refusal stands for</h2>
 *
 * Both reasons the cluster has for refusing - no secondary storage, or credentials which
 * may not read - are permanent, and they have the same consequence for this adapter: it
 * cannot search, so it cannot locate a workflow. The messages naming this state therefore
 * name both instead of claiming the one which is more likely.
 *
 * <h2>What is deliberately not remembered</h2>
 *
 * A cluster which cannot be REACHED while the probe runs is not declared incapable. The
 * answer stays open and the next question asks again, because an unreachable cluster says
 * nothing about what it offers once it is back.
 */
@Slf4j
public class Camunda8QueryApi {

  /**
   * What every message about a cluster refusing to be searched says about the reason,
   * because the cluster refuses for two reasons and separates them in prose only.
   */
  public static final String WHY_THE_CLUSTER_CANNOT_BE_SEARCHED = "either the cluster runs WITHOUT "
      + "secondary storage (camunda.data.secondary-storage.type), or this adapter's credentials "
      + "are not allowed to read what it asks for";

  private final String adapterId;

  private final Supplier<CamundaClient> client;

  /**
   * What the cluster answered, <code>null</code> until a probe got an answer at all. The
   * value cannot change while the application runs: secondary storage and the adapter's
   * credentials are both part of how the cluster respectively the application was
   * started.
   */
  private volatile Boolean answers;

  public Camunda8QueryApi(
      final String adapterId,
      final Supplier<CamundaClient> client) {

    this.adapterId = adapterId;
    this.client = client;

  }

  /**
   * Whether this cluster serves query-API requests, asking it where nothing answered that
   * yet.
   * <p>
   * A search of one page holding one item is the whole question: an empty result is an
   * answer like any other, and a refusal is the only outcome which is not. The search
   * carries no filter on purpose - the cluster refuses the query endpoints as a whole, so
   * a filter would make the answer neither more reliable nor cheaper.
   *
   * @return Whether the cluster answers query-API requests
   */
  public boolean answers() {

    final var remembered = answers;
    if (remembered != null) {
      return remembered;
    }
    try {
      client
          .get()
          .newProcessInstanceSearchRequest()
          .page(page -> page.limit(1))
          .send()
          .join();
      answers = Boolean.TRUE;
    } catch (final Exception e) {
      if (!Camunda8Errors.queryApiRefused(e)) {
        log.debug(
            "Camunda8[{}]: could not find out whether the query API answers - assuming it does",
            adapterId,
            e);
        return true;
      }
      answers = Boolean.FALSE;
    }
    return answers;

  }

}
