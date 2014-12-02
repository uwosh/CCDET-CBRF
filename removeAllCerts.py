import logging
import transaction

logger = logging.getLogger("removeAllCerts")

def removeAllCerts(self):
    logger.info("self is %s" % self)
    context = self
    if self.id != 'cbrf-folder':
        return "Not called from cbrf-folder. Aborting."
    while True:
        ids = context.keys()
        logger.info("there are %s certifications left" % len(ids))
        ids = ids[0:100]
        if len(ids) > 0:
            ids = list(ids)
            context.manage_delObjects(ids)
            logger.info("removed 100 certifications")
            transaction.commit()
        else:
            logger.info("Done")
            break
    return "Done."

