package io.pivotal.gemfire.server.functions;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.pdx.PdxInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.pivotal.gemfire.domain.BeaconResponse;
import io.pivotal.gemfire.server.service.CustomerPromotionService;

@SuppressWarnings("rawtypes")
public class GetBeaconResponse implements Function, Declarable {

	private static final long serialVersionUID = -2607789846026486661L;

	private static final String BEACON_RESPONSE_REGION = "BeaconResponse";

	private final Logger log = LoggerFactory.getLogger(GetBeaconResponse.class);

	private CustomerPromotionService service = new CustomerPromotionService();

	@SuppressWarnings("unchecked")
	public void execute(FunctionContext ctx) {
		RegionFunctionContext rfc = (RegionFunctionContext) ctx;
		String key = (String) rfc.getFilter().iterator().next();
		try {
			rfc.getDataSet().put(key, rfc.getArguments());
			BeaconResponse beaconResponse = service.getCustomerPromotion((PdxInstance) rfc.getArguments());
			CacheFactory.getAnyInstance().getRegion(BEACON_RESPONSE_REGION).put(beaconResponse.getKey(),
					beaconResponse);
			ctx.getResultSender().lastResult(beaconResponse);
		} catch (Exception e) {
			String msg = "Error executing function " + getId() + " exception: " + e.getMessage();
			log.error(msg);
			throw new FunctionException(msg, e);
		}
	}

	public String getId() {
		return GetBeaconResponse.class.getSimpleName();
	}

	public boolean hasResult() {
		return true;
	}

	public boolean isHA() {
		return true;
	}

	public boolean optimizeForWrite() {
		return true;
	}
}
