/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.gms;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.net.InetAddress;

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This module is responsible for Gossiping information for the local endpoint. This abstraction
 * maintains the list of live and dead endpoints. Periodically i.e. every 1 second this module
 * chooses a random node and initiates a round of Gossip with it. A round of Gossip involves 3
 * rounds of messaging. For instance if node A wants to initiate a round of Gossip with node B
 * it starts off by sending node B a GossipDigestSynMessage. Node B on receipt of this message
 * sends node A a GossipDigestAckMessage. On receipt of this message node A sends node B a
 * GossipDigestAck2Message which completes a round of Gossip. This module as and when it hears one
 * of the three above mentioned messages updates the Failure Detector with the liveness information.
 */

public class Gossiper implements IFailureDetectionEventListener, IEndpointStateChangePublisher
{
    private class GossipTimerTask extends TimerTask
    {
        public void run()
        {
            try
            {
                //wait on messaging service to start listening
                MessagingService.instance.waitUntilListening();
                
                /* Update the local heartbeat counter. */
                endpointStateMap_.get(localEndpoint_).getHeartBeatState().updateHeartBeat();
                List<GossipDigest> gDigests = new ArrayList<GossipDigest>();
                Gossiper.instance.makeRandomGossipDigest(gDigests);

                if ( gDigests.size() > 0 )
                {
                    Message message = makeGossipDigestSynMessage(gDigests);
                    /* Gossip to some random live member */
                    boolean gossipedToSeed = doGossipToLiveMember(message);

                    /* Gossip to some unreachable member with some probability to check if he is back up */
                    doGossipToUnreachableMember(message);

                    /* Gossip to a seed if we did not do so above, or we have seen less nodes
                       than there are seeds.  This prevents partitions where each group of nodes
                       is only gossiping to a subset of the seeds.

                       The most straightforward check would be to check that all the seeds have been
                       verified either as live or unreachable.  To avoid that computation each round,
                       we reason that:

                       either all the live nodes are seeds, in which case non-seeds that come online
                       will introduce themselves to a member of the ring by definition,

                       or there is at least one non-seed node in the list, in which case eventually
                       someone will gossip to it, and then do a gossip to a random seed from the
                       gossipedToSeed check.

                       See CASSANDRA-150 for more exposition. */
                    if (!gossipedToSeed || liveEndpoints_.size() < seeds_.size())
                        doGossipToSeed(message);

                    if (logger_.isTraceEnabled())
                        logger_.trace("Performing status check ...");
                    doStatusCheck();
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public final static int intervalInMillis_ = 1000;
    private static Logger logger_ = LoggerFactory.getLogger(Gossiper.class);
    public static final Gossiper instance = new Gossiper();

    private Timer gossipTimer_;
    private InetAddress localEndpoint_;
    private long aVeryLongTime_;
    private long FatClientTimeout_;
    private Random random_ = new Random();
    private Comparator<InetAddress> inetcomparator = new Comparator<InetAddress>()
    {
        public int compare(InetAddress addr1,  InetAddress addr2)
        {
            return addr1.getHostAddress().compareTo(addr2.getHostAddress());
        }
    };

    /* subscribers for interest in EndpointState change */
    private List<IEndpointStateChangeSubscriber> subscribers_ = new CopyOnWriteArrayList<IEndpointStateChangeSubscriber>();

    /* live member set */
    private Set<InetAddress> liveEndpoints_ = new ConcurrentSkipListSet<InetAddress>(inetcomparator);

    /* unreachable member set */
    private Set<InetAddress> unreachableEndpoints_ = new ConcurrentSkipListSet<InetAddress>(inetcomparator);

    /* initial seeds for joining the cluster */
    private Set<InetAddress> seeds_ = new ConcurrentSkipListSet<InetAddress>(inetcomparator);

    /* map where key is the endpoint and value is the state associated with the endpoint */
    Map<InetAddress, EndpointState> endpointStateMap_ = new ConcurrentHashMap<InetAddress, EndpointState>();

    /* map where key is endpoint and value is timestamp when this endpoint was removed from
     * gossip. We will ignore any gossip regarding these endpoints for Streaming.RING_DELAY time
     * after removal to prevent nodes from falsely reincarnating during the time when removal
     * gossip gets propagated to all nodes */
    Map<InetAddress, Long> justRemovedEndpoints_ = new ConcurrentHashMap<InetAddress, Long>();

    private Gossiper()
    {
        gossipTimer_ = new Timer(false);
        // 3 days
        aVeryLongTime_ = 259200 * 1000;
        // 1 hour
        FatClientTimeout_ = 60 * 60 * 1000;
        /* register with the Failure Detector for receiving Failure detector events */
        FailureDetector.instance.registerFailureDetectionEventListener(this);
    }

    /** Register with the Gossiper for EndpointState notifications */
    public void register(IEndpointStateChangeSubscriber subscriber)
    {
        subscribers_.add(subscriber);
    }

    public void unregister(IEndpointStateChangeSubscriber subscriber)
    {
        subscribers_.remove(subscriber);
    }

    public Set<InetAddress> getLiveMembers()
    {
        Set<InetAddress> liveMbrs = new HashSet<InetAddress>(liveEndpoints_);
        liveMbrs.add(localEndpoint_);
        return liveMbrs;
    }

    public Set<InetAddress> getUnreachableMembers()
    {
        return new HashSet<InetAddress>(unreachableEndpoints_);
    }

    /**
     * This method is part of IFailureDetectionEventListener interface. This is invoked
     * by the Failure Detector when it convicts an end point.
     *
     * param @ endpoint end point that is convicted.
    */
    public void convict(InetAddress endpoint)
    {
        EndpointState epState = endpointStateMap_.get(endpoint);
        if (epState.isAlive())
        {
            logger_.info("InetAddress {} is now dead.", endpoint);
            isAlive(endpoint, epState, false);
        }
    }

    int getMaxEndpointStateVersion(EndpointState epState)
    {
        List<Integer> versions = new ArrayList<Integer>();
        versions.add( epState.getHeartBeatState().getHeartBeatVersion() );
        Map<String, ApplicationState> appStateMap = epState.getApplicationStateMap();

        for (ApplicationState value : appStateMap.values())
        {
            int stateVersion = value.getStateVersion();
            versions.add( stateVersion );
        }

        /* sort to get the max version to build GossipDigest for this endpoint */
        Collections.sort(versions);
        int maxVersion = versions.get(versions.size() - 1);
        versions.clear();
        return maxVersion;
    }

    /**
     * Removes the endpoint from unreachable endpoint set
     *
     * @param endpoint endpoint to be removed from the current membership.
    */
    void evictFromMembership(InetAddress endpoint)
    {
        unreachableEndpoints_.remove(endpoint);
    }

    /**
     * Removes the endpoint completely from Gossip
     */
    public void removeEndpoint(InetAddress endpoint)
    {
        liveEndpoints_.remove(endpoint);
        unreachableEndpoints_.remove(endpoint);
        endpointStateMap_.remove(endpoint);
        FailureDetector.instance.remove(endpoint);
        justRemovedEndpoints_.put(endpoint, System.currentTimeMillis());
    }

    /**
     * The gossip digest is built based on randomization
     * rather than just looping through the collection of live endpoints.
     *
     * @param gDigests list of Gossip Digests.
    */
    void makeRandomGossipDigest(List<GossipDigest> gDigests)
    {
        /* Add the local endpoint state */
        EndpointState epState = endpointStateMap_.get(localEndpoint_);
        int generation = epState.getHeartBeatState().getGeneration();
        int maxVersion = getMaxEndpointStateVersion(epState);
        gDigests.add( new GossipDigest(localEndpoint_, generation, maxVersion) );

        List<InetAddress> endpoints = new ArrayList<InetAddress>(endpointStateMap_.keySet());
        Collections.shuffle(endpoints, random_);
        for (InetAddress endpoint : endpoints)
        {
            epState = endpointStateMap_.get(endpoint);
            if (epState != null)
            {
                generation = epState.getHeartBeatState().getGeneration();
                maxVersion = getMaxEndpointStateVersion(epState);
                gDigests.add(new GossipDigest(endpoint, generation, maxVersion));
            }
            else
            {
            	gDigests.add(new GossipDigest(endpoint, 0, 0));
            }
        }

        /* FOR DEBUG ONLY - remove later */
        StringBuilder sb = new StringBuilder();
        for ( GossipDigest gDigest : gDigests )
        {
            sb.append(gDigest);
            sb.append(" ");
        }
        if (logger_.isTraceEnabled())
            logger_.trace("Gossip Digests are : " + sb.toString());
    }

    public boolean isKnownEndpoint(InetAddress endpoint)
    {
        return endpointStateMap_.containsKey(endpoint);
    }

    public int getCurrentGenerationNumber(InetAddress endpoint)
    {
    	return endpointStateMap_.get(endpoint).getHeartBeatState().getGeneration();
    }

    Message makeGossipDigestSynMessage(List<GossipDigest> gDigests) throws IOException
    {
        GossipDigestSynMessage gDigestMessage = new GossipDigestSynMessage(DatabaseDescriptor.getClusterName(), gDigests);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream( bos );
        GossipDigestSynMessage.serializer().serialize(gDigestMessage, dos);
        return new Message(localEndpoint_, StageManager.GOSSIP_STAGE, StorageService.Verb.GOSSIP_DIGEST_SYN, bos.toByteArray());
    }

    Message makeGossipDigestAckMessage(GossipDigestAckMessage gDigestAckMessage) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        GossipDigestAckMessage.serializer().serialize(gDigestAckMessage, dos);
        if (logger_.isTraceEnabled())
            logger_.trace("@@@@ Size of GossipDigestAckMessage is " + bos.toByteArray().length);
        return new Message(localEndpoint_, StageManager.GOSSIP_STAGE, StorageService.Verb.GOSSIP_DIGEST_ACK, bos.toByteArray());
    }

    Message makeGossipDigestAck2Message(GossipDigestAck2Message gDigestAck2Message) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        GossipDigestAck2Message.serializer().serialize(gDigestAck2Message, dos);
        return new Message(localEndpoint_, StageManager.GOSSIP_STAGE, StorageService.Verb.GOSSIP_DIGEST_ACK2, bos.toByteArray());
    }

    /**
     * Returns true if the chosen target was also a seed. False otherwise
     *
     *  @param message message to sent
     *  @param epSet a set of endpoint from which a random endpoint is chosen.
     *  @return true if the chosen endpoint is also a seed.
     */
    boolean sendGossip(Message message, Set<InetAddress> epSet)
    {
        int size = epSet.size();
        /* Generate a random number from 0 -> size */
        List<InetAddress> liveEndpoints = new ArrayList<InetAddress>(epSet);
        int index = (size == 1) ? 0 : random_.nextInt(size);
        InetAddress to = liveEndpoints.get(index);
        if (logger_.isTraceEnabled())
            logger_.trace("Sending a GossipDigestSynMessage to {} ...", to);
        MessagingService.instance.sendOneWay(message, to);
        return seeds_.contains(to);
    }

    /* Sends a Gossip message to a live member and returns true if the recipient was a seed */
    boolean doGossipToLiveMember(Message message)
    {
        int size = liveEndpoints_.size();
        if ( size == 0 )
            return false;
        // return sendGossipToLiveNode(message);
        /* Use this for a cluster size >= 30 */
        return sendGossip(message, liveEndpoints_);
    }

    /* Sends a Gossip message to an unreachable member */
    void doGossipToUnreachableMember(Message message)
    {
        double liveEndpoints = liveEndpoints_.size();
        double unreachableEndpoints = unreachableEndpoints_.size();
        if ( unreachableEndpoints > 0 )
        {
            /* based on some probability */
            double prob = unreachableEndpoints / (liveEndpoints + 1);
            double randDbl = random_.nextDouble();
            if ( randDbl < prob )
                sendGossip(message, unreachableEndpoints_);
        }
    }

    /* Gossip to a seed for facilitating partition healing */
    void doGossipToSeed(Message message)
    {
        int size = seeds_.size();
        if ( size > 0 )
        {
            if ( size == 1 && seeds_.contains(localEndpoint_) )
            {
                return;
            }

            if ( liveEndpoints_.size() == 0 )
            {
                sendGossip(message, seeds_);
            }
            else
            {
                /* Gossip with the seed with some probability. */
                double probability = seeds_.size() / (double)( liveEndpoints_.size() + unreachableEndpoints_.size() );
                double randDbl = random_.nextDouble();
                if ( randDbl <= probability )
                    sendGossip(message, seeds_);
            }
        }
    }

    void doStatusCheck()
    {
        long now = System.currentTimeMillis();

        Set<InetAddress> eps = endpointStateMap_.keySet();
        for ( InetAddress endpoint : eps )
        {
            if ( endpoint.equals(localEndpoint_) )
                continue;

            FailureDetector.instance.interpret(endpoint);
            EndpointState epState = endpointStateMap_.get(endpoint);
            if ( epState != null )
            {
                long duration = now - epState.getUpdateTimestamp();

                // check if this is a fat client. fat clients are removed automatically from
                // gosip after FatClientTimeout
                if (!epState.getHasToken() && !epState.isAlive() && (duration > FatClientTimeout_))
                {
                    if (StorageService.instance.getTokenMetadata().isMember(endpoint))
                        epState.setHasToken(true);
                    else
                    {
                        logger_.info("FatClient " + endpoint + " has been silent for " + FatClientTimeout_ + "ms, removing from gossip");
                        removeEndpoint(endpoint);
                    }
                }

                if ( !epState.isAlive() && (duration > aVeryLongTime_) )
                {
                    evictFromMembership(endpoint);
                }
            }

            if (!justRemovedEndpoints_.isEmpty())
            {
                Hashtable<InetAddress, Long> copy = new Hashtable<InetAddress, Long>(justRemovedEndpoints_);
                for (Map.Entry<InetAddress, Long> entry : copy.entrySet())
                {
                    if ((now - entry.getValue()) > StorageService.RING_DELAY)
                    {
                        if (logger_.isDebugEnabled())
                            logger_.debug(StorageService.RING_DELAY + " elapsed, " + entry.getKey() + " gossip quarantine over");
                        justRemovedEndpoints_.remove(entry.getKey());
                    }
                }
            }
        }
    }

    EndpointState getEndpointStateForEndpoint(InetAddress ep)
    {
        return endpointStateMap_.get(ep);
    }

    EndpointState getStateForVersionBiggerThan(InetAddress forEndpoint, int version)
    {
        if (logger_.isTraceEnabled())
            logger_.trace("Scanning for state greater than " + version + " for " + forEndpoint);
        EndpointState epState = endpointStateMap_.get(forEndpoint);
        EndpointState reqdEndpointState = null;

        if ( epState != null )
        {
            /*
             * Here we try to include the Heart Beat state only if it is
             * greater than the version passed in. It might happen that
             * the heart beat version maybe lesser than the version passed
             * in and some application state has a version that is greater
             * than the version passed in. In this case we also send the old
             * heart beat and throw it away on the receiver if it is redundant.
            */
            int localHbVersion = epState.getHeartBeatState().getHeartBeatVersion();
            if ( localHbVersion > version )
            {
                reqdEndpointState = new EndpointState(epState.getHeartBeatState());
            }
            Map<String, ApplicationState> appStateMap = epState.getApplicationStateMap();
            /* Accumulate all application states whose versions are greater than "version" variable */
            for (Entry<String, ApplicationState> entry : appStateMap.entrySet())
            {
                ApplicationState appState = entry.getValue();
                if ( appState.getStateVersion() > version )
                {
                    if ( reqdEndpointState == null )
                    {
                        reqdEndpointState = new EndpointState(epState.getHeartBeatState());
                    }
                    final String key = entry.getKey();
                    if (logger_.isTraceEnabled())
                        logger_.trace("Adding state " + key + ": " + appState.getValue());
                    reqdEndpointState.addApplicationState(key, appState);
                }
            }
        }
        return reqdEndpointState;
    }

    /** determine which endpoint started up earlier */
    public int compareEndpointStartup(InetAddress addr1, InetAddress addr2)
    {
        EndpointState ep1 = getEndpointStateForEndpoint(addr1);
        EndpointState ep2 = getEndpointStateForEndpoint(addr2);
        assert ep1 != null && ep2 != null;
        return ep1.getHeartBeatState().getGeneration() - ep2.getHeartBeatState().getGeneration();
    }    

    void notifyFailureDetector(List<GossipDigest> gDigests)
    {
        IFailureDetector fd = FailureDetector.instance;
        for ( GossipDigest gDigest : gDigests )
        {
            EndpointState localEndpointState = endpointStateMap_.get(gDigest.endpoint_);
            /*
             * If the local endpoint state exists then report to the FD only
             * if the versions workout.
            */
            if ( localEndpointState != null )
            {
                int localGeneration = endpointStateMap_.get(gDigest.endpoint_).getHeartBeatState().generation_;
                int remoteGeneration = gDigest.generation_;
                if ( remoteGeneration > localGeneration )
                {
                    fd.report(gDigest.endpoint_);
                    continue;
                }

                if ( remoteGeneration == localGeneration )
                {
                    int localVersion = getMaxEndpointStateVersion(localEndpointState);
                    //int localVersion = endpointStateMap_.get(gDigest.endpoint_).getHeartBeatState().getHeartBeatVersion();
                    int remoteVersion = gDigest.maxVersion_;
                    if ( remoteVersion > localVersion )
                    {
                        fd.report(gDigest.endpoint_);
                    }
                }
            }
        }
    }

    void notifyFailureDetector(Map<InetAddress, EndpointState> remoteEpStateMap)
    {
        IFailureDetector fd = FailureDetector.instance;
        for (Entry<InetAddress, EndpointState> entry : remoteEpStateMap.entrySet())
        {
            InetAddress endpoint = entry.getKey();
            EndpointState remoteEndpointState = entry.getValue();
            EndpointState localEndpointState = endpointStateMap_.get(endpoint);
            /*
             * If the local endpoint state exists then report to the FD only
             * if the versions workout.
            */
            if ( localEndpointState != null )
            {
                int localGeneration = localEndpointState.getHeartBeatState().generation_;
                int remoteGeneration = remoteEndpointState.getHeartBeatState().generation_;
                if ( remoteGeneration > localGeneration )
                {
                    fd.report(endpoint);
                    continue;
                }

                if ( remoteGeneration == localGeneration )
                {
                    int localVersion = getMaxEndpointStateVersion(localEndpointState);
                    //int localVersion = localEndpointState.getHeartBeatState().getHeartBeatVersion();
                    int remoteVersion = remoteEndpointState.getHeartBeatState().getHeartBeatVersion();
                    if ( remoteVersion > localVersion )
                    {
                        fd.report(endpoint);
                    }
                }
            }
        }
    }

    void markAlive(InetAddress addr, EndpointState localState)
    {
        if (logger_.isTraceEnabled())
            logger_.trace("marking as alive {}", addr);
        if ( !localState.isAlive() )
        {
            isAlive(addr, localState, true);
            logger_.info("InetAddress {} is now UP", addr);
        }
    }

    private void handleNewJoin(InetAddress ep, EndpointState epState)
    {
        if (justRemovedEndpoints_.containsKey(ep))
            return;
    	logger_.info("Node {} is now part of the cluster", ep);
        handleMajorStateChange(ep, epState, false);
    }

    private void handleGenerationChange(InetAddress ep, EndpointState epState)
    {
        logger_.info("Node {} has restarted, now UP again", ep);
        handleMajorStateChange(ep, epState, true);
    }

    /**
     * This method is called whenever there is a "big" change in ep state (either a previously
     * unknown node or a generation change for a known node). If the node is new, it will be
     * initially marked as dead. It will be marked alive as soon as another piece of gossip
     * arrives. On the other hand if the node is already known (generation change), we will
     * immediately mark it alive.
     *
     * @param ep endpoint
     * @param epState EndpointState for the endpoint
     * @param isKnownNode is this node familiar to us already (present in endpointStateMap)
     */
    private void handleMajorStateChange(InetAddress ep, EndpointState epState, boolean isKnownNode)
    {
        endpointStateMap_.put(ep, epState);
        isAlive(ep, epState, isKnownNode);
        for (IEndpointStateChangeSubscriber subscriber : subscribers_)
            subscriber.onJoin(ep, epState);
    }

    void applyStateLocally(Map<InetAddress, EndpointState> epStateMap)
    {
        for (Entry<InetAddress, EndpointState> entry : epStateMap.entrySet())
        {
            InetAddress ep = entry.getKey();
            if ( ep.equals( localEndpoint_ ) )
                continue;

            EndpointState localEpStatePtr = endpointStateMap_.get(ep);
            EndpointState remoteState = entry.getValue();
            /*
                If state does not exist just add it. If it does then add it only if the version
                of the remote copy is greater than the local copy.
            */
            if ( localEpStatePtr != null )
            {
            	int localGeneration = localEpStatePtr.getHeartBeatState().getGeneration();
            	int remoteGeneration = remoteState.getHeartBeatState().getGeneration();

            	if (remoteGeneration > localGeneration)
            	{
                    handleGenerationChange(ep, remoteState);
            	}
            	else if ( remoteGeneration == localGeneration )
            	{
	                /* manage the membership state */
	                int localMaxVersion = getMaxEndpointStateVersion(localEpStatePtr);
	                int remoteMaxVersion = getMaxEndpointStateVersion(remoteState);
	                if ( remoteMaxVersion > localMaxVersion )
	                {
	                    markAlive(ep, localEpStatePtr);
	                    applyHeartBeatStateLocally(ep, localEpStatePtr, remoteState);
	                    /* apply ApplicationState */
	                    applyApplicationStateLocally(ep, localEpStatePtr, remoteState);
	                }
            	}
            }
            else
            {
            	handleNewJoin(ep, remoteState);
            }
        }
    }

    void applyHeartBeatStateLocally(InetAddress addr, EndpointState localState, EndpointState remoteState)
    {
        HeartBeatState localHbState = localState.getHeartBeatState();
        HeartBeatState remoteHbState = remoteState.getHeartBeatState();

        if ( remoteHbState.getGeneration() > localHbState.getGeneration() )
        {
            localState.setHeartBeatState(remoteHbState);
        }
        if ( localHbState.getGeneration() == remoteHbState.getGeneration() )
        {
            if ( remoteHbState.getHeartBeatVersion() > localHbState.getHeartBeatVersion() )
            {
                int oldVersion = localHbState.getHeartBeatVersion();
                localState.setHeartBeatState(remoteHbState);
                if (logger_.isTraceEnabled())
                    logger_.trace("Updating heartbeat state version to " + localState.getHeartBeatState().getHeartBeatVersion() + " from " + oldVersion + " for " + addr + " ...");
            }
        }
    }

    void applyApplicationStateLocally(InetAddress addr, EndpointState localStatePtr, EndpointState remoteStatePtr)
    {
        Map<String, ApplicationState> localAppStateMap = localStatePtr.getApplicationStateMap();

        for (Map.Entry<String,ApplicationState> remoteEntry : remoteStatePtr.getApplicationStateMap().entrySet())
        {
            String remoteKey = remoteEntry.getKey();
            ApplicationState remoteAppState = remoteEntry.getValue();
            ApplicationState localAppState = localAppStateMap.get(remoteKey);

            /* If state doesn't exist locally for this key then just apply it */
            if ( localAppState == null )
            {
                localStatePtr.addApplicationState(remoteKey, remoteAppState);
                doNotifications(addr, remoteKey, remoteAppState);
                continue;
            }

            int remoteGeneration = remoteStatePtr.getHeartBeatState().getGeneration();
            int localGeneration = localStatePtr.getHeartBeatState().getGeneration();
            assert remoteGeneration >= localGeneration; // SystemTable makes sure we never generate a smaller generation on start

            /* If the remoteGeneration is greater than localGeneration then apply state blindly */
            if ( remoteGeneration > localGeneration )
            {
                localStatePtr.addApplicationState(remoteKey, remoteAppState);
                doNotifications(addr, remoteKey, remoteAppState);
                continue;
            }

            /* If the generations are the same then apply state if the remote version is greater than local version. */
            if ( remoteGeneration == localGeneration )
            {
                int remoteVersion = remoteAppState.getStateVersion();
                int localVersion = localAppState.getStateVersion();

                if ( remoteVersion > localVersion )
                {
                    localStatePtr.addApplicationState(remoteKey, remoteAppState);
                    doNotifications(addr, remoteKey, remoteAppState);
                }
            }
        }
    }

    void doNotifications(InetAddress addr, String stateName, ApplicationState state)
    {
        for (IEndpointStateChangeSubscriber subscriber : subscribers_)
        {
            subscriber.onChange(addr, stateName, state);
        }
    }

    void isAlive(InetAddress addr, EndpointState epState, boolean value)
    {
        epState.isAlive(value);
        if (value)
        {
            liveEndpoints_.add(addr);
            unreachableEndpoints_.remove(addr);
            for (IEndpointStateChangeSubscriber subscriber : subscribers_)
                subscriber.onAlive(addr, epState);
        }
        else
        {
            liveEndpoints_.remove(addr);
            unreachableEndpoints_.add(addr);
            for (IEndpointStateChangeSubscriber subscriber : subscribers_)
                subscriber.onDead(addr, epState);
        }
        if (epState.isAGossiper())
            return;
        epState.isAGossiper(true);
    }

    /* Request all the state for the endpoint in the gDigest */
    void requestAll(GossipDigest gDigest, List<GossipDigest> deltaGossipDigestList, int remoteGeneration)
    {
        /* We are here since we have no data for this endpoint locally so request everthing. */
        deltaGossipDigestList.add( new GossipDigest(gDigest.getEndpoint(), remoteGeneration, 0) );
    }

    /* Send all the data with version greater than maxRemoteVersion */
    void sendAll(GossipDigest gDigest, Map<InetAddress, EndpointState> deltaEpStateMap, int maxRemoteVersion)
    {
        EndpointState localEpStatePtr = getStateForVersionBiggerThan(gDigest.getEndpoint(), maxRemoteVersion) ;
        if ( localEpStatePtr != null )
            deltaEpStateMap.put(gDigest.getEndpoint(), localEpStatePtr);
    }

    /*
        This method is used to figure the state that the Gossiper has but Gossipee doesn't. The delta digests
        and the delta state are built up.
    */
    void examineGossiper(List<GossipDigest> gDigestList, List<GossipDigest> deltaGossipDigestList, Map<InetAddress, EndpointState> deltaEpStateMap)
    {
        for ( GossipDigest gDigest : gDigestList )
        {
            int remoteGeneration = gDigest.getGeneration();
            int maxRemoteVersion = gDigest.getMaxVersion();
            /* Get state associated with the end point in digest */
            EndpointState epStatePtr = endpointStateMap_.get(gDigest.getEndpoint());
            /*
                Here we need to fire a GossipDigestAckMessage. If we have some data associated with this endpoint locally
                then we follow the "if" path of the logic. If we have absolutely nothing for this endpoint we need to
                request all the data for this endpoint.
            */
            if ( epStatePtr != null )
            {
                int localGeneration = epStatePtr.getHeartBeatState().getGeneration();
                /* get the max version of all keys in the state associated with this endpoint */
                int maxLocalVersion = getMaxEndpointStateVersion(epStatePtr);
                if ( remoteGeneration == localGeneration && maxRemoteVersion == maxLocalVersion )
                    continue;

                if ( remoteGeneration > localGeneration )
                {
                    /* we request everything from the gossiper */
                    requestAll(gDigest, deltaGossipDigestList, remoteGeneration);
                }
                if ( remoteGeneration < localGeneration )
                {
                    /* send all data with generation = localgeneration and version > 0 */
                    sendAll(gDigest, deltaEpStateMap, 0);
                }
                if ( remoteGeneration == localGeneration )
                {
                    /*
                        If the max remote version is greater then we request the remote endpoint send us all the data
                        for this endpoint with version greater than the max version number we have locally for this
                        endpoint.
                        If the max remote version is lesser, then we send all the data we have locally for this endpoint
                        with version greater than the max remote version.
                    */
                    if ( maxRemoteVersion > maxLocalVersion )
                    {
                        deltaGossipDigestList.add( new GossipDigest(gDigest.getEndpoint(), remoteGeneration, maxLocalVersion) );
                    }
                    if ( maxRemoteVersion < maxLocalVersion )
                    {
                        /* send all data with generation = localgeneration and version > maxRemoteVersion */
                        sendAll(gDigest, deltaEpStateMap, maxRemoteVersion);
                    }
                }
            }
            else
            {
                /* We are here since we have no data for this endpoint locally so request everything. */
                requestAll(gDigest, deltaGossipDigestList, remoteGeneration);
            }
        }
    }

    /**
     * Start the gossiper with the generation # retrieved from the System
     * table
     */
    public void start(InetAddress localEndpoint, int generationNbr)
    {
        localEndpoint_ = localEndpoint;
        /* Get the seeds from the config and initialize them. */
        Set<InetAddress> seedHosts = DatabaseDescriptor.getSeeds();
        for (InetAddress seed : seedHosts)
        {
            if (seed.equals(localEndpoint))
                continue;
            seeds_.add(seed);
        }

        /* initialize the heartbeat state for this localEndpoint */
        EndpointState localState = endpointStateMap_.get(localEndpoint_);
        if ( localState == null )
        {
            HeartBeatState hbState = new HeartBeatState(generationNbr);
            localState = new EndpointState(hbState);
            localState.isAlive(true);
            localState.isAGossiper(true);
            endpointStateMap_.put(localEndpoint_, localState);
        }

        /* starts a timer thread */
        gossipTimer_.schedule( new GossipTimerTask(), Gossiper.intervalInMillis_, Gossiper.intervalInMillis_);
    }

    public void addLocalApplicationState(String key, ApplicationState appState)
    {
        assert !StorageService.instance.isClientMode();
        EndpointState epState = endpointStateMap_.get(localEndpoint_);
        assert epState != null;
        epState.addApplicationState(key, appState);
    }

    public void stop()
    {
        gossipTimer_.cancel();
        gossipTimer_ = new Timer(false); // makes the Gossiper reentrant.
    }
}
