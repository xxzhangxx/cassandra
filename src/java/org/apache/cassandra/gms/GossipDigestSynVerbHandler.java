package org.apache.cassandra.gms;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class GossipDigestSynVerbHandler implements IVerbHandler
{
    private static Logger logger_ = LoggerFactory.getLogger( GossipDigestSynVerbHandler.class);

    public void doVerb(Message message)
    {
        InetAddress from = message.getFrom();
        if (logger_.isTraceEnabled())
            logger_.trace("Received a GossipDigestSynMessage from {}", from);

        byte[] bytes = message.getMessageBody();
        DataInputStream dis = new DataInputStream( new ByteArrayInputStream(bytes) );

        try
        {
            GossipDigestSynMessage gDigestMessage = GossipDigestSynMessage.serializer().deserialize(dis);
            /* If the message is from a different cluster throw it away. */
            if ( !gDigestMessage.clusterId_.equals(DatabaseDescriptor.getClusterName()) )
            {
                logger_.warn("ClusterName mismatch from " + from + " " + gDigestMessage.clusterId_  + "!=" + DatabaseDescriptor.getClusterName());
                return;
            }

            List<GossipDigest> gDigestList = gDigestMessage.getGossipDigests();
            /* Notify the Failure Detector */
            Gossiper.instance.notifyFailureDetector(gDigestList);

            doSort(gDigestList);

            List<GossipDigest> deltaGossipDigestList = new ArrayList<GossipDigest>();
            Map<InetAddress, EndpointState> deltaEpStateMap = new HashMap<InetAddress, EndpointState>();
            Gossiper.instance.examineGossiper(gDigestList, deltaGossipDigestList, deltaEpStateMap);

            GossipDigestAckMessage gDigestAck = new GossipDigestAckMessage(deltaGossipDigestList, deltaEpStateMap);
            Message gDigestAckMessage = Gossiper.instance.makeGossipDigestAckMessage(gDigestAck);
            if (logger_.isTraceEnabled())
                logger_.trace("Sending a GossipDigestAckMessage to {}", from);
            MessagingService.instance.sendOneWay(gDigestAckMessage, from);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /*
     * First construct a map whose key is the endpoint in the GossipDigest and the value is the
     * GossipDigest itself. Then build a list of version differences i.e difference between the
     * version in the GossipDigest and the version in the local state for a given InetAddress.
     * Sort this list. Now loop through the sorted list and retrieve the GossipDigest corresponding
     * to the endpoint from the map that was initially constructed.
    */
    private void doSort(List<GossipDigest> gDigestList)
    {
        /* Construct a map of endpoint to GossipDigest. */
        Map<InetAddress, GossipDigest> epToDigestMap = new HashMap<InetAddress, GossipDigest>();
        for ( GossipDigest gDigest : gDigestList )
        {
            epToDigestMap.put(gDigest.getEndpoint(), gDigest);
        }

        /*
         * These digests have their maxVersion set to the difference of the version
         * of the local EndpointState and the version found in the GossipDigest.
        */
        List<GossipDigest> diffDigests = new ArrayList<GossipDigest>();
        for ( GossipDigest gDigest : gDigestList )
        {
            InetAddress ep = gDigest.getEndpoint();
            EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
            int version = (epState != null) ? Gossiper.instance.getMaxEndpointStateVersion( epState ) : 0;
            int diffVersion = Math.abs(version - gDigest.getMaxVersion() );
            diffDigests.add( new GossipDigest(ep, gDigest.getGeneration(), diffVersion) );
        }

        gDigestList.clear();
        Collections.sort(diffDigests);
        int size = diffDigests.size();
        /*
         * Report the digests in descending order. This takes care of the endpoints
         * that are far behind w.r.t this local endpoint
        */
        for ( int i = size - 1; i >= 0; --i )
        {
            gDigestList.add( epToDigestMap.get(diffDigests.get(i).getEndpoint()) );
        }
    }
}
