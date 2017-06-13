package com.netflix.simianarmy.aws.janitor.rule.volume;

import java.util.Date;

import org.apache.commons.lang.Validate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;

import com.netflix.simianarmy.Resource;
import com.netflix.simianarmy.aws.AWSResource;
import com.netflix.simianarmy.janitor.JanitorMonkey;

public abstract class AbstractRule {

    /**
     * The date format used to print or parse the user specified termination date.
     **/
    protected static final DateTimeFormatter TERMINATION_DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");

    protected Boolean isResourceValid(Resource resource) {
        Validate.notNull(resource);
        if (!resource.getResourceType().name().equals("EBS_VOLUME")) {
            return true;
        }

        // The state of the volume being "available" means that it is not attached to any instance.
        if (!"available".equals(((AWSResource) resource).getAWSResourceState())) {
            return true;
        }
        String janitorTag = resource.getTag(JanitorMonkey.JANITOR_TAG);
        if (janitorTag != null) {
            if ("donotmark".equals(janitorTag)) {
                getLogger().info("The volume {} is tagged as not handled by Janitor", resource.getId());
                return true;
            }
            try {
                setResourceJanitorData(resource, janitorTag);
                return false;
            } catch (Exception e) {
                getLogger().error(String.format("The janitor tag is not a user specified date: %s", janitorTag));
            }
        }
        return null;
    }

    private void setResourceJanitorData(Resource resource, String janitorTag) {
        // Owners can tag the volume with a termination date in the "janitor" tag.
        Date userSpecifiedDate = new Date(TERMINATION_DATE_FORMATTER.parseDateTime(janitorTag).getMillis());
        resource.setExpectedTerminationTime(userSpecifiedDate);
        resource.setTerminationReason(String.format("User specified termination date %s", janitorTag));
    }

    protected abstract Logger getLogger();
}
