/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.concurrent.akka;


import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.javadsl.Flow;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.stream.stage.TimerGraphStageLogic;
import com.google.common.base.Supplier;
import scala.concurrent.duration.FiniteDuration;

import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

/**
 * This is a {@link Flow} that does not emit until a predicate is true or the buffered elements are
 * evaluated to value higher than a maximum or a finite duration has passed.
 */
public class BatchFlow<E, A> extends GraphStage<FlowShape<E, A>> {

  public final Inlet<E> in = Inlet.create("in");
  public final Outlet<A> out = Outlet.create("out");
  private final int maxCost;
  private final FiniteDuration period;
  private final Predicate<E> predicate;
  private final ToIntFunction<E> costFunction;
  private final Supplier<A> zero;
  private final BiFunction<A, E, A> aggregate;
  private final FlowShape<E, A> shape = FlowShape.of(in, out);
  private static final String MY_TIMER_KEY = "key";

  public BatchFlow(int maxCost, FiniteDuration period,
      Predicate<E> predicate,
      ToIntFunction<E> costFunction,
      Supplier<A> zero,
      BiFunction<A, E, A> aggregate) {
    this.maxCost = maxCost;
    this.period = period;
    this.predicate = predicate;
    this.costFunction = costFunction;
    this.zero = zero;
    this.aggregate = aggregate;
  }

  @Override
  public FlowShape<E, A> shape() {
    return shape;
  }

  @Override
  public GraphStageLogic createLogic(Attributes inheritedAtts) {
    return new TimerGraphStageLogic(shape) {
      private A acum = zero.get();
      /**
       * True iff buff is not empty AND (timer fired OR group is full OR predicate is true)
       */
      private boolean groupClosed = false;
      private boolean groupEmitted = false;
      private boolean finished = false;
      private int acumCost = 0;

      {
        setHandler(in, new AbstractInHandler() {
          @Override
          public void onPush() throws Exception {
            if (!groupClosed) {
              nextElement(grab(in));
            }
          }

          @Override
          public void onUpstreamFinish() throws Exception {
            finished = true;
            if (groupEmitted) {
              completeStage();
            } else {
              closeGroup();
            }
          }
        });
        setHandler(out, new AbstractOutHandler() {
          @Override
          public void onPull() throws Exception {
            if (groupClosed) {
              emitGroup();
            }
          }
        });
      }

      @Override
      public void preStart() {
        schedulePeriodically(MY_TIMER_KEY, period);
        pull(in);
      }

      @Override
      public void onTimer(Object timerKey) {
        assert timerKey.equals(MY_TIMER_KEY);
        if (acumCost > 0) {
          closeGroup();
        }
      }

      private void nextElement(E elem) {
        groupEmitted = false;
        acum = aggregate.apply(acum, elem);
        acumCost += costFunction.applyAsInt(elem);
        if (acumCost >= maxCost || predicate.test(elem)) {
          schedulePeriodically(MY_TIMER_KEY, period);
          closeGroup();
        } else {
          pull(in);
        }
      }

      private void closeGroup() {
        groupClosed = true;
        if (isAvailable(out)) {
          emitGroup();
        }
      }

      private void emitGroup() {
        groupEmitted = true;
        push(out, acum);
        acum = null;
        if (!finished) {
          startNewGroup();
        } else {
          completeStage();
        }
      }

      private void startNewGroup() {
        acum = zero.get();
        acumCost = 0;
        groupClosed = false;
        if (isAvailable(in)) {
          nextElement(grab(in));
        } else {
          if (!hasBeenPulled(in)) {
            pull(in);
          }
        }
      }
    };
  }

}
