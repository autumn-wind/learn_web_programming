//=============================================================================
/**
 *      Copyright (c) Freewheel, 2007-2009. All rights reserved.
 *
 *      @file
 *
 *      @author jack <jackdeng@freewheel.tv>
 *
 *      @brief
 *
 *      Revision History:
 *              2007/09/24      jack
 *                      Created.
 *
 *      Last Update:
 */
//=============================================================================

#include "cajun/json/elements.h"
#include "cajun/json/writer.h"

#include "protobuf/Log_Record.pb.h"
#include "protobuf/User_Log_Record.pb.h"
#include "protobuf/Win_Notice_Revenue_Chain.pb.h"
#include "protobuf/Fastlane_Log_Record.pb.h"

#include "Ads_Selector.h"
#include "Ads_Request.Param.h"

#include "Ads_Request.h"
#include "Ads_Response.h"
#include "Ads_Server.h"
#include "Ads_Pusher.h"

#include "Ads_Enhanced_Shared_Data_Service.Client.h"
#include "Ads_Log_Service.h"

#include "Ads_User_Info.h"
#include "Ads_User_Agent_Parser.h"
#include "Ads_UA_Parser.h"
#include "Ads_Macro_Expander.h"

#include "Ads_Pusher_Base.h"
#include "Ads_Cacher.h"
#include "Ads_Sanity_Test.h"

#include "Ads_Selector.Slot.h"
#include "Ads_Selector.UEC.h"
#include "Ads_Selector.CBP.h"
#include "Ads_Selector.MKPL.h"
#include "Ads_Selector.MKPL.Avails.h"
#include "Ads_Selector.Sponsorship.h"
#include "Ads_Selector.Data_Privacy.h"
#include "Ads_Selector.Data_Right_Management.h"
#include "Ads_Selector.FC.h"
#include "Ads_Selector.Budget_Control.h"
#include "Ads_Selector.External_Router.h"
#include "Ads_Selector.Bidding_Pricing_Strategy.h"
#include "Ads_Selector.JiTT.h"
#include "Ads_Selector.Error.h"

#include "server/Ads_XML_Node.h"
#include "server/Ads_Request.OpenRTB.h"
#include "server/Ads_Request.VOD_Router.h"
#include "server/Ads_Error_Stats.h"
#include "server/Ads_VOD_Service_Assurance.h"
#include "server/Ads_Delta_Repo_Loader.h"

#include "Ads_CCH_Client.h"
#include "Ads_Aerospike_Client.h"
#include "limits.h"
#include "Ads_Monitor.h"

#if defined(ADS_ENABLE_FORECAST)
#include "forecast/research/xtr/gen/xTRService_types.h"
#include "forecast/research/xtr/gen/xtr.h"
#include "3rd/thrift/src/thrift/transport/TSocket.h"
#include "3rd/thrift/src/thrift/transport/TBufferTransports.h"
#include "3rd/thrift/src/thrift/transport/TTransportException.h"
#include "3rd/thrift/src/thrift/protocol/TBinaryProtocol.h"
#include "server/Ads_XTR_Client.h"
#include "3rd/thrift/src/thrift/TToString.h"
#endif

#define DAA_OPTOUT_TOKEN_TTL 3600
#define MAX_ACCEPTABLE_WASTED_DURATION_OF_SLOT 15

#include "Ads_Selector.External_Info.h"
#include "Ads_Selector.External_Ad.h"
#include "Ads_Selector.SSP.h"
#include "Ads_Selector.PG.h"
#include "Ads_Selector.SSP.Clearing_Price.h"
#include "Ads_Selector.SSP.Crypto.h"
#include "Ads_Selector.Scheduler.h"
#include "Ads_Selector.Audience.h"
#include "Ads_Selector.Measurement.h"
#include "Ads_Selector.FOD_Lookup.h"

using measurement::PLATFORM;
using selection::Ads_Owner_XDevice_Vendor_Repo;
using selection::Pacing_Status;
using selection::PACING_STATUS_TYPE;

#define EXEC_IO(tag) \
	if (reschedule(ctx, tag)) return 1

#if defined(ADS_ENABLE_FORECAST)
// xtr
int
generate_xtr_message(Ads_Selection_Context *ctx, xtr::xTR_Message &xtr_message, Ads_XTR_Client::xTR_Function_Stat& xtr_stat_info, bool& cpx_present)
{
	const Ads_Asset *root_asset = ctx->root_asset_;
	if (root_asset)
	{
		xtr_message.root_asset_id = ads::entity::id(root_asset->id_);
		xtr_message.root_network_id = ads::entity::id(root_asset->network_id_);
	}
	else
	{
		const Ads_Asset_Base *root_section = ctx->root_section_;
		if (root_section)
			xtr_message.root_network_id = ads::entity::id(root_section->network_id_);
		else if (ads::entity::is_valid_id(ctx->network_id_))
			xtr_message.root_network_id = ads::entity::id(ctx->network_id_);
	}
	if (ads::entity::is_valid_id(ctx->section_id_))
		xtr_message.root_section_id = ads::entity::id(ctx->section_id_);
	xtr_message.user_id = ctx->request_->id_repo_.user_id().id();
	if (ads::entity::is_valid_id(ctx->network_id_))
		xtr_message.distributor_id = ads::entity::id(ctx->network_id_);
	if (ctx->asset_duration_ >= 0)
		xtr_message.asset_duration = ads::i64_to_str(ctx->asset_duration_);
	xtr_message.city = ads::entity::str(ctx->request_->city_id());
	xtr_message.country = ads::entity::str(ctx->request_->country_id());
	xtr_message.state_id = ads::entity::id(ctx->request_->state_id());
	xtr_message.dma_code = ads::entity::id(ctx->request_->dma_code());
	xtr_message.parsed_user_agent = ctx->request_->parsed_user_agent_;
	if (!(ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::SKIP_GENERATE_RESPONSE)) // only set tid in real scenario
		xtr_message.transaction_id = ctx->transaction_id_;

	std::map<int, xtr::Slot> slots;
	std::map<Ads_GUID, int> slots_max_position;
	int index_count = 0;

	for (std::list<Ads_Advertisement_Candidate_Ref *>::const_iterator it = ctx->delivered_candidates_.begin(); it != ctx->delivered_candidates_.end(); ++it)
	{
		Ads_Advertisement_Candidate_Ref *candidate_ref = *it;
		const Ads_Advertisement_Candidate* candidate = candidate_ref->candidate_;
		Ads_Slot_Base *slot = candidate_ref->slot_;
		ADS_ASSERT(slot);
		if (!slot || !slot->ad_unit_ || slot->env() == ads::ENV_PAGE || slot->env() == ads::ENV_PLAYER) continue; // skip displacing

		ADS_DEBUG((LP_DEBUG, "xtr add ad : ad_id(%s), index(%d), ad_unit_id(%s), env(%d) , custom_id(%s)\n",
					ADS_ENTITY_ID_CSTR(candidate->ad_->id_),
					slot->index(),
					ADS_ENTITY_ID_CSTR(slot->ad_unit_->id_),
					slot->env(),
					slot->custom_id_.c_str()));

		if (slot->xTR_index_ <=0) slot->xTR_index_ = ++index_count;
		if (slots.find(slot->xTR_index_) == slots.end())
		{
			ADS_DEBUG((LP_DEBUG, "xtr add slot : x_index(%d), ad_unit_id(%s), env(%d) , custom_id(%s)\n",
					slot->xTR_index_,
					ADS_ENTITY_ID_CSTR(slot->ad_unit_->id_),
					slot->env(),
					slot->custom_id_.c_str()));
			xtr::Slot s;
			s.slot_id = slot->xTR_index_;
			s.sequence = 0; //0 for non-video
			s.ad_unit_id = ads::entity::id(slot->ad_unit_->id_);
			if (slot->env() == ads::ENV_VIDEO)
			{
				Ads_Video_Slot* vslot = reinterpret_cast<Ads_Video_Slot *>(slot);
				if (vslot && !vslot->time_position_class_.empty())
				{
					s.time_position = vslot->time_position_;
					s.time_position_class = vslot->time_position_class_;
					if (vslot->standard_ad_unit_sequence_ > 0)
						s.sequence = vslot->standard_ad_unit_sequence_ - 1;
				}
			}
			slots[slot->xTR_index_] = s;
			slots_max_position[slot->xTR_index_] = 0;
		}

		if (candidate->is_CPx_)
		{
			xtr::Candidate_Advertisement xtr_ad;
			xtr_ad.ad_id = ads::entity::id(candidate->ad_->id_);
                	xtr_ad.position_in_slot = candidate_ref->position_in_slot_;
                	xtr_ad.ad_unit_id = ads::entity::id(candidate->ad_unit()->id_);
                	xtr_ad.duration = candidate_ref->duration();

                	xtr_ad.concrete_event_id[candidate->triggering_concrete_event_id_] = -1; // cpx
                	xtr_ad.concrete_event_id[0] = -1; // ack ratio
			slots[slot->xTR_index_].ads.push_back(xtr_ad);
			cpx_present = true;
		}
		if (slots_max_position[slot->xTR_index_] < candidate_ref->position_in_slot_)
		{
			slots_max_position[slot->xTR_index_] = candidate_ref->position_in_slot_;
		}
	}

	std::vector<xtr::Slot> xtr_slots;

	for(std::map<int, xtr::Slot>::iterator it = slots.begin(); it != slots.end(); ++it)
	{
		xtr::Slot& s = it->second;
		if (!(ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::SKIP_GENERATE_RESPONSE))  //at Real round fake -1 AD to fetch SLOT xtr ratio
		{
			for (int i = 0; i <= slots_max_position[s.slot_id]; i++)
			{
				xtr::Candidate_Advertisement fake_ad;
				fake_ad.ad_id = -1;
				fake_ad.ad_unit_id = s.ad_unit_id;
				fake_ad.position_in_slot = i; //fake at least one position
				fake_ad.duration = -1;
				fake_ad.concrete_event_id[0] = -1; //only get ack_ratio, for cpx, will use ad level event ratio
				s.ads.push_back(fake_ad);

				ADS_DEBUG((LP_DEBUG,
						"xtr add fake add for slot: slot_id(%d),index(%d), time_position_class(%s) \n",
						s.slot_id,
						s.sequence,
						s.time_position_class.c_str()
						));
			}
		}
		xtr_slots.push_back(s);
	}
	xtr_message.slots = xtr_slots;

	xtr_stat_info.n_xtr_cpx_reqs_ += cpx_present ? 1 : 0;
	++xtr_stat_info.n_xtr_reqs_;
	xtr_stat_info.n_xtr_slots_ += xtr_slots.size();
	for (size_t i = 0; i < xtr_slots.size(); ++i)
	{
		xtr_stat_info.n_xtr_ads_ += xtr_slots[i].ads.size();
	}

	//send monitor to pqm
	const Ads_String timezone = Ads_Monitor::forecast_simulate_stats_get_timezone();
	Ads_Monitor::Labels labels = {
		{"sub_group", "forecast"},
		{"timezone", timezone},
		{"network", ADS_ENTITY_ID_STR(xtr_message.root_network_id)}
		};
	Ads_Monitor::forecast_xtr_stats_set(labels, "xtr_request_total", xtr_stat_info.n_xtr_reqs_, {"xtr_request_total"});

	return 0;
}

int
parse_xtr_message(Ads_Selection_Context *ctx, xtr::xTR_Message &xtr_message)
{
	// key: ad_id, slot_sequence, position_in_slot
	// value: ratio map<type_id, ratio>
	std::map<std::pair<Ads_GUID, std::pair<int16_t, int16_t> >, std::map<int16_t, double> > ratios;

	for (std::vector<xtr::Slot>::const_iterator it = xtr_message.slots.begin(); it != xtr_message.slots.end();++it)
	{
		for (std::vector<xtr::Candidate_Advertisement>::const_iterator ait = it->ads.begin(); ait != it->ads.end(); ++ait)
		{
			std::pair<Ads_GUID, std::pair<int16_t, int16_t> > key = std::make_pair(ait->ad_id, std::make_pair(it->slot_id, ait->position_in_slot));
			ratios[key] = ait->concrete_event_id;
			if (Ads_Server_Config::instance()->enable_debug_)
			{
				ADS_DEBUG((LP_DEBUG,
							"xtr get msg : ad_id(%d), index(%d) , position in slot(%d)\n ",
							ait->ad_id,
							it->sequence,
							ait->position_in_slot
						  ));
				for (std::map<int16_t, double>::iterator debug_it = ratios[key].begin(); debug_it != ratios[key].end(); ++debug_it)
				{
					ADS_DEBUG((LP_DEBUG, "  --  xtr value: event_id(%d) , ratio(%f)\n", debug_it->first, debug_it->second));
				}
			}
		}
	}
	//store ad level ratio
	for (std::list<Ads_Advertisement_Candidate_Ref *>::iterator it = ctx->delivered_candidates_.begin(); it != ctx->delivered_candidates_.end(); ++it)
	{
		Ads_Advertisement_Candidate_Ref *candidate_ref = *it;
		const Ads_Advertisement_Candidate* candidate = candidate_ref->candidate_;
		Ads_Slot_Base *slot = candidate_ref->slot_;
		ADS_ASSERT(slot);
		if (!slot) continue;
		std::pair<Ads_GUID, std::pair<int, int> > key = std::make_pair(ads::entity::id(candidate->ad_->id_), std::make_pair(slot->xTR_index_, candidate_ref->position_in_slot_));
		if (ratios.find(key) != ratios.end())
			candidate_ref->xtr_ratios_ = ratios[key];
	}

	//store slot level ratio
	for (std::deque<Ads_Slot_Base *>::iterator it = ctx->request_slots_.begin(); it != ctx->request_slots_.end(); ++it)
	{
		Ads_Slot_Base * slot = *it;
		int ret_position = 0;
		std::pair<Ads_GUID, std::pair<int, int> > key;
		while (ret_position < Ads_Video_Slot::MAX_POSITION)
		{
			key = std::make_pair(ads::entity::id(-1), std::make_pair(slot->xTR_index_, ret_position));
			if (ratios.find(key) != ratios.end())
			{
				slot->xTR_position_ratios[ret_position] = ratios[key];
			}
			else break;
			ret_position++;
		}
	}
	return 0;
}
#endif

		void Ads_Selector::Retry_Scheduler::pend_for_retry(Ads_Advertisement_Candidate* candidate)
		{
			pend_candidates_.emplace_back(candidate);
		}

		void Ads_Selector::Retry_Scheduler::reactivate()
		{
			retry_candidates_.insert(pend_candidates_.begin(), pend_candidates_.end());
			pend_candidates_.clear();
		}

		Ads_Advertisement_Candidate* Ads_Selector::Retry_Scheduler::get_next_candidate()
		{
			if (retry_candidates_.empty())
			{
				if (orig_candidate_offset_ < orig_candidate_size_)
					return original_candidate_[orig_candidate_offset_++];
				else
					return 0;
			}
			else
			{
				Ads_Advertisement_Candidate *result = *retry_candidates_.begin();
				retry_candidates_.erase(retry_candidates_.begin());
				return result;
			}
		}

		const Ads_Advertisement_Candidate_Vector& Ads_Selector::Retry_Scheduler::get_pend_candidates() const
		{
			return pend_candidates_;
		}

		void Ads_Selector::Stats::log_selection(const Ads_Selection_Context * ctx)
		{
			int country_id = ads::entity::id(ctx->request_->country_id());
			int state_id = -1;

			///US state
			if (country_id == 165)
			{
				state_id = ads::entity::id(ctx->request_->state_id());
				state_id -=  US_STATE_BASE;
			}

			mutex_.acquire();

			if (ctx->request_->method() == Ads_Request::POST) ++n_selection_post_;
			else ++n_selection_get_;

			if (ctx->is_tracking()) ++n_selection_tracking_;
			if (ctx->response_->format() == Ads_Response::LEGACY) ++n_response_legacy_;

			n_user_cache_hit_ += ctx->n_user_cache_hit_;
			n_user_hit_ += ctx->n_user_hit_;
			n_user_missed_ += ctx->n_user_missed_;

			if (country_id >= 0 && country_id < MAX_COUNTRIES)
				++n_selection_per_country_[country_id];

			if (state_id >= 0 && state_id < MAX_US_STATES)
				++n_selection_per_state_[state_id];

			//distributor
			Network_Stats& ns = per_network_[ctx->request_->network_id()];
			if (ctx->request_->method() == Ads_Request::POST)
				++ns.n_request_post_;
			else ++ns.n_request_get_;

			//resellers
			for (Ads_GUID_Set::const_iterator it = ctx->log_reseller_networks_.begin(); it != ctx->log_reseller_networks_.end(); ++it)
			{
				Ads_GUID network_id = *it;
				Network_Stats& ns = per_network_[network_id];
				++ns.n_request_reseller_;
			}

			if (ctx->enable_deep_lookup_)
			{
				if (ctx->assets_) ++ns.n_asset_cache_hit_;
				else ++ns.n_asset_cache_miss_;
				if(ctx->enable_site_section_deep_lookup_)
				{
					if(ctx->site_sections_) ++ns.n_section_cache_hit_;
					else ++ns.n_section_cache_miss_;
				}
			}

			//CRO
			Ads_GUID root_network_id = ctx->root_network_id();
			if (ads::entity::is_valid_id(root_network_id))
			{
				Network_Stats& ns = per_network_[root_network_id];
				if (ctx->request_->method() == Ads_Request::POST)
					++ns.n_selection_post_;
				else ++ns.n_selection_get_;
			}

			if (ctx->request_->custom_counter_id() >= 0)
			{
				Network_Stats& ns = per_network_[ctx->request_->custom_counter_id()];
				if (ctx->request_->method() == Ads_Request::POST)
					++ns.n_selection_post_;
				else ++ns.n_selection_get_;

				if (ctx->enable_deep_lookup_)
				{
					if (ctx->assets_) ++ns.n_asset_cache_hit_;
					else ++ns.n_asset_cache_miss_;
					if(ctx->enable_site_section_deep_lookup_)
					{
						if(ctx->site_sections_) ++ns.n_section_cache_hit_;
						else ++ns.n_section_cache_miss_;
					}
				}

				// reserved
				if (ctx->request_->custom_counter_id() < 10000)
				{
					int i = ctx->now_ - timestamp_;
					if (i >= 0 && i < (int)Custom_Stats::capacity())
					{
						Custom_Stats& ns = custom_stats_[ctx->request_->custom_counter_id()];
						if (ctx->request_->method() == Ads_Request::POST)
							++ns.n_selection_post_[i];
						else ++ns.n_selection_get_[i];

						ns.size(i + 1);
					}
				}
			}

			mutex_.release();
		}

		void Ads_Selector::Stats::log_ack(bool is_impression, bool is_video_view, bool is_click, bool has_redirect)
		{
			mutex_.acquire();

			++this->n_ack_;
			if (is_impression) ++this->n_impression_;
			else if (is_video_view) ++this->n_video_view_;
			else if (is_click) ++this->n_click_;

			if (has_redirect) ++this->n_redirect_;

			mutex_.release();
		}

		void Ads_Selector::Stats::log_ack(Ads_GUID network_id, bool is_impression, bool is_video_view, bool is_click, bool has_redirect)
		{
			mutex_.acquire();

			Network_Stats& ns = per_network_[network_id];
			++ns.n_ack_;
			if (is_impression) ++ns.n_impression_;
			else if (is_video_view) ++ns.n_video_view_;
			else if (is_click) ++ns.n_click_;

			if (has_redirect) ++ns.n_redirect_;

			mutex_.release();
		}

		int Ads_Selector::Stats::to_json(json::ObjectP& obj)
		{
			mutex_.acquire();

			obj["timestamp"] = json::Number(timestamp_);
			obj["seconds"] = json::Number(n_seconds_);
			obj["ptiling"] = json::Number(n_ptiling_);
			obj["ptiling_failed"] = json::Number(n_ptiling_failed_);
			obj["selection_get"] = json::Number(n_selection_get_);
			obj["selection_post"] = json::Number(n_selection_post_);
			obj["selection_skipped"] = json::Number(n_selection_skipped_);
			obj["selection_failed"] = json::Number(n_selection_failed_);
			obj["selection_tracking"] = json::Number(n_selection_tracking_);
			obj["selection_playlist"] = json::Number(n_selection_playlist_);//TODO HLS_DEPRECATION
			obj["selection_slow"] = json::Number(n_selection_slow_);
			obj["selection_slow_300ms"] = json::Number(n_selection_slow_300ms_);
			obj["selection_slow_150ms"] = json::Number(n_selection_slow_150ms_);
			obj["selection_slow_q_user"] = json::Number(n_selection_slow_q_user_);
			obj["selection_slow_q_asset"] = json::Number(n_selection_slow_q_asset_);
			obj["selection_slow_q_state"] = json::Number(n_selection_slow_q_state_);
			obj["request_playlist"] = json::Number(n_request_playlist_);
			obj["request_playlist_failed"] = json::Number(n_request_playlist_failed_);
			obj["request_playlist_skipped"] = json::Number(n_request_playlist_skipped_);

			obj["request_playlist_espn"] = json::Number(n_request_playlist_espn_);
			obj["request_playlist_failed_espn"] = json::Number(n_request_playlist_failed_espn_);
			obj["request_playlist_skipped_espn"] = json::Number(n_request_playlist_skipped_espn_);

			obj["canoe_smsi_message_highly_delayed"] = json::Number(n_canoe_smsi_message_highly_delayed_);
			obj["canoe_smsi_impression_duplicated"] = json::Number(n_canoe_smsi_impression_duplicated_);

			obj["response_legacy"] = json::Number(n_response_legacy_);
			obj["user_cache_hit"] = json::Number(n_user_cache_hit_);
			obj["user_hit"] = json::Number(n_user_hit_);
			obj["user_missed"] = json::Number(n_user_missed_);
			obj["ack"] = json::Number(n_ack_);
			obj["ack_invalid"] = json::Number(n_ack_invalid_);
			obj["click"] = json::Number(n_click_);
			obj["impression"] = json::Number(n_impression_);
			obj["video_view"] = json::Number(n_video_view_);
			obj["redirect"] = json::Number(n_redirect_);

			json::ObjectP countries;
			for (size_t i = 0; i < MAX_COUNTRIES; ++i)
			{
				if (n_selection_per_country_[i])
					countries["c" + ads::i64_to_str(i)] = json::Number(n_selection_per_country_[i]);
			}
			obj["selection_countries"] = countries;

			json::ObjectP states;
			for (size_t i = 0; i < MAX_US_STATES; ++i)
			{
				if (n_selection_per_state_[i])
					states["s" + ads::i64_to_str(i + US_STATE_BASE)] = json::Number(n_selection_per_state_[i]);
			}
			obj["selection_states"] = states;

			json::ObjectP networks;
			for (std::map<Ads_GUID, Network_Stats>::iterator it = per_network_.begin();
				it != per_network_.end();
				++it)
			{
				const Network_Stats& ns = it->second;
				json::ObjectP n;
				if (ns.n_request_get_> 0) n["selection_get"] = json::Number(ns.n_request_get_);
				if (ns.n_request_post_ > 0) n["selection_post"] = json::Number(ns.n_request_post_);
				if (ns.n_request_reseller_ > 0) n["selection_reseller"] = json::Number(ns.n_request_reseller_);
				if (ns.n_selection_get_> 0) n["request_get"] = json::Number(ns.n_selection_get_);
				if (ns.n_selection_post_ > 0) n["request_post"] = json::Number(ns.n_selection_post_);
				if (ns.n_ack_ > 0) n["ack"] = json::Number(ns.n_ack_);
				if (ns.n_click_ > 0) n["click"] = json::Number(ns.n_click_);
				if (ns.n_impression_ > 0) n["impression"] = json::Number(ns.n_impression_);
				if (ns.n_video_view_ > 0) n["video_view"] = json::Number(ns.n_video_view_);
				if (ns.n_redirect_ > 0) n["redirect"] = json::Number(ns.n_redirect_);
				if (ns.n_asset_cache_hit_ > 0) n["asset_cache_hit"] = json::Number(ns.n_asset_cache_hit_);
				if (ns.n_asset_cache_miss_ > 0) n["asset_cache_miss"] = json::Number(ns.n_asset_cache_miss_);
				if (ns.n_section_cache_hit_ > 0) n["section_cache_hit"] = json::Number(ns.n_section_cache_hit_);
				if (ns.n_section_cache_miss_ > 0) n["section_cache_miss"] = json::Number(ns.n_section_cache_miss_);

				networks["n" + ADS_ENTITY_ID_STR(it->first)] = n;
			}
			obj["per_network"] = networks;

			if (!custom_stats_.empty())
			{
				json::ObjectP custom;
				for (std::map<Ads_GUID, Custom_Stats>::iterator it = custom_stats_.begin(); it != custom_stats_.end(); ++it)
				{
					const Custom_Stats& ns = it->second;
					json::ObjectP c;
					for (int i = 0; i < ns.size_; ++i)
					{
						json::ObjectP n;
						n["selection_get"] = json::Number(ns.n_selection_get_[i]);
						n["selection_post"] = json::Number(ns.n_selection_post_[i]);
						c["t" + ads::i64_to_str(i + timestamp_)] = n;
					}

					custom["n" + ADS_ENTITY_ID_STR(it->first)] = c;
				}
				obj["custom"] = custom;
			}

			mutex_.release();
			return 0;
		}

	void Ads_Selector::log_selection(const Ads_Selection_Context * ctx)
	{
		Ads_Monitor::stats_inc(Ads_Monitor::USER_CACHE_HIT, ctx->n_user_cache_hit_);
		// TODO: deprecate n_user_hit_ and n_user_missed_

		if (ctx->request_->method() == Ads_Request::POST)
		{
			Ads_Monitor::stats_inc(Ads_Monitor::TX_SELECTION_POST);
		} else
		{
			Ads_Monitor::stats_inc(Ads_Monitor::TX_SELECTION_GET);
		}
		Ads_Monitor::stats_inc(Ads_Monitor::TX_SELECTION_TOTAL);
		if (ctx->response_->format() == Ads_Response::LEGACY)
		{
			Ads_Monitor::stats_inc(Ads_Monitor::RESPONSE_FORMAT_AD);
		}
		if (ctx->is_tracking())
		{
			Ads_Monitor::stats_inc(Ads_Monitor::REQUEST_TRACKING);
		}

		int country_id = ads::entity::id(ctx->request_->country_id());
		int state_id = -1;
		///US state
		if (country_id == 165)
		{
			state_id = ads::entity::id(ctx->request_->state_id());
			state_id -=  Server_Stats::US_STATE_BASE;
		}
		if (country_id >= 0 && country_id < Server_Stats::MAX_COUNTRIES)
			++server_stats_.n_selection_per_country_[country_id];

		if (state_id >= 0 && state_id < Server_Stats::MAX_US_STATES)
			++server_stats_.n_selection_per_state_[state_id];

		Ads_GUID distributor_network_id = ctx->request_->network_id();
		if (ctx->repository_->system_config().is_network_selection_stats_enabled(distributor_network_id))
		{
			Server_Stats::Network_Stats& ns = server_stats_.network_stats(distributor_network_id);
			++ns.n_request_total_;
			Ads_Monitor::network_stats_inc(Ads_Monitor::NetworkStats::NETWORK_REQUEST_TOTAL, distributor_network_id);
			if (ctx->request_->method() == Ads_Request::POST)
			{
				++ns.n_request_post_;
				Ads_Monitor::network_stats_inc(Ads_Monitor::NetworkStats::NETWORK_REQUEST_POST, distributor_network_id);
			}
			else
			{
				++ns.n_request_get_;
				Ads_Monitor::network_stats_inc(Ads_Monitor::NetworkStats::NETWORK_REQUEST_GET, distributor_network_id);
			}
		}

		Ads_GUID root_network_id = ctx->root_network_id();
		if (ads::entity::is_valid_id(root_network_id) && ctx->repository_->system_config().is_network_selection_stats_enabled(root_network_id))
		{
			Server_Stats::Network_Stats& ns = server_stats_.network_stats(root_network_id);
			++ns.n_selection_total_;
			Ads_Monitor::network_stats_inc(Ads_Monitor::NetworkStats::NETWORK_SELECTION_TOTAL, root_network_id);
			if (ctx->request_->method() == Ads_Request::POST)
			{
				++ns.n_selection_post_;
				Ads_Monitor::network_stats_inc(Ads_Monitor::NetworkStats::NETWORK_SELECTION_POST, root_network_id);
			}
			else
			{
				++ns.n_selection_get_;
				Ads_Monitor::network_stats_inc(Ads_Monitor::NetworkStats::NETWORK_SELECTION_GET, root_network_id);
			}
		}
	}

	void Ads_Selector::log_ack(bool is_impression, bool is_video_view, bool is_click, bool has_redirect)
	{
		Ads_Monitor::stats_inc(Ads_Monitor::ACK_TOTAL);
		if (is_impression)
		{
			Ads_Monitor::stats_inc(Ads_Monitor::ACK_IMPRESSION);
		}
		else if (is_video_view)
		{
			Ads_Monitor::stats_inc(Ads_Monitor::ACK_VIDEO_VIEW);
		}
		else if (is_click)
		{
			Ads_Monitor::stats_inc(Ads_Monitor::ACK_CLICK);
		}

		if (has_redirect)
		{
			Ads_Monitor::stats_inc(Ads_Monitor::ACK_REDIRECT);
		}
	}

	void Ads_Selector::log_ack(Ads_GUID network_id, bool is_impression, bool is_video_view)
	{
		Server_Stats::Network_Stats& ns = server_stats_.network_stats(network_id);
		++ns.n_ack_;
		Ads_Monitor::network_stats_inc(Ads_Monitor::NetworkStats::NETWORK_ACK, network_id);
		if (is_impression)
		{
			++ns.n_impression_;
			Ads_Monitor::network_stats_inc(Ads_Monitor::NetworkStats::NETWORK_IMPRESSION, network_id);
		}
		else if (is_video_view)
		{
			++ns.n_video_view_;
			Ads_Monitor::network_stats_inc(Ads_Monitor::NetworkStats::NETWORK_VIDEO_VIEW, network_id);
		}
	}

	void Ads_Selector::log_selection_asset_not_found(const Ads_Selection_Context* ctx, bool is_additional)
	{
		Ads_Monitor::stats_inc(is_additional ? Ads_Monitor::TX_SELECTION_ADDITIONAL_ASSET_NOT_FOUND : Ads_Monitor::TX_SELECTION_ASSET_NOT_FOUND);
		if (ctx->asset_network_ && ctx->repository_->system_config().is_network_selection_stats_enabled(ctx->asset_network_->id_))
		{
			Server_Stats::Network_Stats& ns = server_stats_.network_stats(ctx->asset_network_->id_);
			if (is_additional)
			{
				++ns.n_additional_asset_not_found_;
				Ads_Monitor::network_stats_inc(Ads_Monitor::NetworkStats::NETWORK_ADDITIONAL_ASSET_NOT_FOUND, ctx->asset_network_->id_);
			}
			else
			{
				++ns.n_asset_not_found_;
				Ads_Monitor::network_stats_inc(Ads_Monitor::NetworkStats::NETWORK_ASSET_NOT_FOUND, ctx->asset_network_->id_);
			}
		}
	}

	void Ads_Selector::log_selection_section_not_found(const Ads_Selection_Context* ctx)
	{
		Ads_Monitor::stats_inc(Ads_Monitor::TX_SELECTION_SECTION_NOT_FOUND);
		if (ctx->section_network_ && ctx->repository_->system_config().is_network_selection_stats_enabled(ctx->section_network_->id_))
		{
			Server_Stats::Network_Stats& ns = server_stats_.network_stats(ctx->section_network_->id_);
			++ns.n_section_not_found_;
			Ads_Monitor::network_stats_inc(Ads_Monitor::NetworkStats::NETWORK_SECTION_NOT_FOUND, ctx->section_network_->id_);
		}
	}

	void Ads_Selector::log_no_video_slot(const Ads_Slot_List& video_slots, Ads_Selection_Context& ctx) const
	{
		bool video_slot_exist = false;
		for (const auto& slot : video_slots)
		{
			const Ads_Video_Slot* vslot = reinterpret_cast<const Ads_Video_Slot *>(slot);
			ADS_ASSERT(vslot != nullptr);

			if (vslot->flags_ & Ads_Slot_Base::FLAG_REMOVED_BY_UX)
			{
				ctx.decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_10);
			}

			if ((vslot->flags_ & Ads_Slot_Base::FLAG_TRACKING_ONLY)
			    || (vslot->flags_ & Ads_Slot_Base::FLAG_PLACEHOLDER))
				continue;

			video_slot_exist = true;
			ctx.decision_info_.add_values(Decision_Info::INDEX_5);
		}

		if (!video_slot_exist)
		{
			ADS_DEBUG((LP_DEBUG, "no available video slots in this transaction\n"));
			ctx.decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_13);
			ctx.add_error(Error::GENERAL, Error::NO_VALID_VIDEO_SLOTS);
		}
	}

	void Ads_Selector::update_metrics()
	{
		Ads_Monitor::stats_set(Ads_Monitor::PENDING_SLOW_SELECTION, this->num_slow_selections());
	}

	void Ads_Selector::log_latency_metrics(Ads_Selection_Context* ctx)
	{
		//TODO to be deprecated
		if (ctx->audience_->time_userdb_user_lookup_ > 100)
		{
			++this->stats_.n_selection_slow_q_user_;
			Ads_Monitor::stats_inc(Ads_Monitor::SLOW_USERDB_LOOKUP_100MS);
		}

		if (ctx->time_asset_lookup_ > 100)
		{
			++this->stats_.n_selection_slow_q_asset_;
			Ads_Monitor::stats_inc(Ads_Monitor::SLOW_ASSET_LOOKUP_100MS);
		}
		if (ctx->time_state_lookup_ > 100)
		{
			++this->stats_.n_selection_slow_q_state_;
			Ads_Monitor::stats_inc(Ads_Monitor::SLOW_SSUS_LOOKUP_100MS);
		}

		auto time_selection = ctx->apm_.calculate_duration_io(ctx);

		Ads_Monitor::stats_observe(Ads_Monitor::EXTERNAL_INFO_LOOKUP_TIME, ctx->time_external_info_lookup_ms_);
		if (time_selection > 50)
		{
			Ads_Monitor::stats_inc(Ads_Monitor::LATENCY_SELECTION);
		}
		if (ctx->audience_->time_userdb_user_lookup_ > 20)
		{
			Ads_Monitor::stats_inc(Ads_Monitor::LATENCY_USERDB_LOOKUP);
		}
		if (ctx->time_state_lookup_ > 20)
		{
			Ads_Monitor::stats_inc(Ads_Monitor::LATENCY_SSUS_LOOKUP);
		}
		if (ctx->time_hls_content_manifest_load_ > 1000)
		{
			Ads_Monitor::stats_inc(Ads_Monitor::EX_HLS_CONTENT_MANIFEST_TIMEOUT);
		}
		if (ctx->time_hls_ad_manifest_load_ > 2000)
		{
			Ads_Monitor::stats_inc(Ads_Monitor::EX_HLS_AD_MANIFEST_TIMEOUT);
		}

		Ads_Monitor::stats_observe(Ads_Monitor::SLOW_SELECTION, ctx->time_elapsed_);
		if (ctx->time_elapsed_ > 500)
		{
			++this->stats_.n_selection_slow_;
			ADS_LOG((LP_ERROR, "request takes %d ms (userdb user lookup: %dms, signal break lookup %dms, mvpd sis info lookup %dms, selection %dms) to complete. transaction_id: %s. user_id: %s. referrer: %s\n",
				ctx->time_elapsed_, ctx->audience_->time_userdb_user_lookup_,
				ctx->time_userdb_signal_break_lookup_, ctx->audience_->time_sis_user_lookup_,
				time_selection, ctx->transaction_id_.c_str(), ctx->user_id().c_str(),
				ctx->request_->virtual_referrer().substr(0, 4096).c_str()));

			if (!Ads_Server_Config::instance()->disable_timeout_logging_)
				ctx->add_error(Error::GENERAL, Error::TIMEOUT);
			if (ctx->time_elapsed_ > 1000)
			{
				Ads_Error_Stats::instance()->err(Ads_Error_Stats::ERR_TX_SELECTION_1S);
			}
			else
			{
				Ads_Error_Stats::instance()->err(Ads_Error_Stats::ERR_TX_SELECTION_500MS);
			}
		}
		else if (ctx->time_elapsed_ > 300)
		{
			++this->stats_.n_selection_slow_300ms_;
			Ads_Error_Stats::instance()->err(Ads_Error_Stats::ERR_TX_SELECTION_300MS);
		}
		else if (ctx->time_elapsed_ > 150)
		{
			++this->stats_.n_selection_slow_150ms_;
		}
	}

	void Ads_Selector::reset_canoe_smsi_message_stats()
	{
		stats_.mutex_.acquire();

		stats_.n_canoe_smsi_message_highly_delayed_ = 0;
		stats_.n_canoe_smsi_impression_duplicated_  = 0;

		stats_.mutex_.release();
	}

	void Ads_Selector::add_canoe_smsi_message_highly_delayed(size_t n)
	{
		stats_.mutex_.acquire();
		stats_.n_canoe_smsi_message_highly_delayed_ += n;
		stats_.mutex_.release();
	}

	void Ads_Selector::add_canoe_smsi_impression_duplicated(size_t n)
	{
		stats_.mutex_.acquire();
		stats_.n_canoe_smsi_impression_duplicated_ += n;
		stats_.mutex_.release();
	}

	int Ads_Selector::update_stats(json::ObjectP& obj)
	{
		time_t now = ::time(NULL);

		if (stats_per_second_.timestamp_ >= now) return 0;

		stats_.mutex_.acquire();
		stats_per_second_.mutex_.acquire();

		stats_per_second_.timestamp_ = stats_.timestamp_;
		stats_per_second_.n_seconds_ = now - stats_per_second_.timestamp_;
	//		stats_per_second_.timestamp_ = now;

		stats_per_second_.n_ptiling_ = stats_.n_ptiling_; stats_.n_ptiling_ = 0;
		stats_per_second_.n_ptiling_failed_ = stats_.n_ptiling_failed_; stats_.n_ptiling_failed_ = 0;
		stats_per_second_.n_selection_get_ = stats_.n_selection_get_; stats_.n_selection_get_ = 0;
		stats_per_second_.n_selection_post_ = stats_.n_selection_post_; stats_.n_selection_post_ = 0;
		stats_per_second_.n_selection_skipped_ = stats_.n_selection_skipped_; stats_.n_selection_skipped_ = 0;
		stats_per_second_.n_selection_failed_ = stats_.n_selection_failed_; stats_.n_selection_failed_ = 0;
		stats_per_second_.n_selection_tracking_ = stats_.n_selection_tracking_; stats_.n_selection_tracking_ = 0;
		stats_per_second_.n_selection_playlist_ = stats_.n_selection_playlist_; stats_.n_selection_playlist_ = 0;
		stats_per_second_.n_selection_slow_ = stats_.n_selection_slow_; stats_.n_selection_slow_ = 0;
		stats_per_second_.n_selection_slow_300ms_ = stats_.n_selection_slow_300ms_; stats_.n_selection_slow_300ms_ = 0;
		stats_per_second_.n_selection_slow_150ms_ = stats_.n_selection_slow_150ms_; stats_.n_selection_slow_150ms_ = 0;
		stats_per_second_.n_selection_slow_q_user_ = stats_.n_selection_slow_q_user_; stats_.n_selection_slow_q_user_ = 0;
		stats_per_second_.n_selection_slow_q_asset_ = stats_.n_selection_slow_q_asset_; stats_.n_selection_slow_q_asset_ = 0;
		stats_per_second_.n_selection_slow_q_state_ = stats_.n_selection_slow_q_state_; stats_.n_selection_slow_q_state_ = 0;
		stats_per_second_.n_request_playlist_ = stats_.n_request_playlist_; stats_.n_request_playlist_ = 0;
		stats_per_second_.n_request_playlist_failed_ = stats_.n_request_playlist_failed_; stats_.n_request_playlist_failed_ = 0;
		stats_per_second_.n_request_playlist_skipped_ = stats_.n_request_playlist_skipped_; stats_.n_request_playlist_skipped_ = 0;

		stats_per_second_.n_request_playlist_espn_ = stats_.n_request_playlist_espn_; stats_.n_request_playlist_espn_ = 0;
		stats_per_second_.n_request_playlist_failed_espn_ = stats_.n_request_playlist_failed_espn_; stats_.n_request_playlist_failed_espn_ = 0;
		stats_per_second_.n_request_playlist_skipped_espn_ = stats_.n_request_playlist_skipped_espn_; stats_.n_request_playlist_skipped_espn_ = 0;

		// Reset them daily by timer service
		stats_per_second_.n_canoe_smsi_message_highly_delayed_ = stats_.n_canoe_smsi_message_highly_delayed_;
		stats_per_second_.n_canoe_smsi_impression_duplicated_  = stats_.n_canoe_smsi_impression_duplicated_;

		stats_per_second_.n_response_legacy_ = stats_.n_response_legacy_; stats_.n_response_legacy_ = 0;
		stats_per_second_.n_user_cache_hit_ = stats_.n_user_cache_hit_; stats_.n_user_cache_hit_ = 0;
		stats_per_second_.n_user_hit_ = stats_.n_user_hit_; stats_.n_user_hit_ = 0;
		stats_per_second_.n_user_missed_ = stats_.n_user_missed_; stats_.n_user_missed_ = 0;

		stats_per_second_.n_ack_ = stats_.n_ack_; stats_.n_ack_ = 0;
		stats_per_second_.n_ack_invalid_ = stats_.n_ack_invalid_; stats_.n_ack_invalid_ = 0;
		stats_per_second_.n_click_ = stats_.n_click_; stats_.n_click_ = 0;
		stats_per_second_.n_impression_ = stats_.n_impression_; stats_.n_impression_ = 0;
		stats_per_second_.n_video_view_ = stats_.n_video_view_; stats_.n_video_view_ = 0;
		stats_per_second_.n_redirect_ = stats_.n_redirect_; stats_.n_redirect_ = 0;

		memcpy(stats_per_second_.n_selection_per_country_, stats_.n_selection_per_country_, sizeof(stats_.n_selection_per_country_));
		memset(stats_.n_selection_per_country_, 0, sizeof(stats_.n_selection_per_country_));
		memcpy(stats_per_second_.n_selection_per_state_, stats_.n_selection_per_state_, sizeof(stats_.n_selection_per_state_));
		memset(stats_.n_selection_per_state_, 0, sizeof(stats_.n_selection_per_state_));

		stats_per_second_.per_network_.swap(stats_.per_network_);
		std::map<Ads_GUID, Stats::Network_Stats> tn;
		tn.swap(stats_.per_network_);

		stats_per_second_.custom_stats_.swap(stats_.custom_stats_);
		std::map<Ads_GUID, Stats::Custom_Stats> tc;
		tc.swap(stats_.custom_stats_);

		stats_.timestamp_ = now;

		stats_per_second_.mutex_.release();
		stats_.mutex_.release();

		stats_per_second_.to_json(obj);
		obj["pending_slow_selection"] = json::Number(this->num_slow_selections());

		return 0;
	}


	const Ads_Creative_Rendition* Ads_Selector::preferred_rendition(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate *candidate, const Creative_Rendition_List& renditions)
	{
		const Ads_Creative_Rendition *rendition = 0;
		double dpr_diff = -1;
		double dpr = 1.0;
		int asset_bit_rate = -1;

		if (ctx->request_ && ctx->request_->rep() && ctx->request_->rep()->key_values_.device_pixel_ratio() != 1.0)
		{
			dpr = ctx->request_->rep()->key_values_.device_pixel_ratio();
		}

		if (ctx->request_ && ctx->request_->rep() && ctx->request_->rep()->key_values_.bitrate_ != -1)
		{
			asset_bit_rate = ctx->request_->rep()->key_values_.bitrate_;
		}

		Creative_Rendition_List vast_renditions;
		size_t rendition_ad_asset_store_bitrate = size_t(-1);
		for (Creative_Rendition_List::const_iterator it = renditions.begin();
				it != renditions.end();
				++it)
		{
			const Ads_Creative_Rendition *arendition = *it;
			ADS_ASSERT(arendition);


			if(candidate->ad_ && candidate->ad_->price_type_ == Ads_Advertisement::FIXED)
			{
				if (ctx->check_bitrate_)
				{// Only for VOD now
					if (asset_bit_rate > 0)
					{
						//OPP-1651
						if (arendition->is_vast_rendition())
						{
							vast_renditions.push_back(arendition);
							continue;
						}
						if (ctx->ad_asset_stores_.empty())
						{
							ALOG(LP_DEBUG, "check_bitrate is true while no valid asset store in request.\n");
							continue;
						}
						Ads_Ad_Asset_Store_Info_Map::const_iterator sit = arendition->ad_asset_store_info_.find(ctx->ad_asset_stores_[0]);
						if (sit == arendition->ad_asset_store_info_.end())
						{
							ALOG(LP_DEBUG, "can not find the asset store from request(id:%d) for rendition.\n", ctx->ad_asset_stores_[0]);
							continue;
						}

						if (!rendition || sit->second.bitrate_ > rendition_ad_asset_store_bitrate)
						{
							rendition = arendition;
							rendition_ad_asset_store_bitrate = sit->second.bitrate_;
							ALOG(LP_DEBUG, "current creative id=%s, rendition bitrate=%d\n", ADS_ENTITY_ID_CSTR(rendition->creative_id_), rendition->bitrate_);
						}
					}
					else
					{
						ALOG(LP_DEBUG, "entertainment asset bitrate is not set\n");
						return 0;
					}
				}
				else {
					if (!rendition || arendition->preference_ > rendition->preference_)
					{
						rendition = arendition;
						if (dpr_diff < 0) dpr_diff = fabs(rendition->device_pixel_ratio_ - dpr);
					}
					else if ((arendition->preference_ == rendition->preference_
								&& fabs(arendition->device_pixel_ratio_ - dpr) < dpr_diff))
					{
						rendition = arendition;
						dpr_diff = fabs(rendition->device_pixel_ratio_ - dpr);
					}
					else if(rendition->rendition_ext_info_ && rendition->rendition_ext_info_->rendition_used_)
					{
						rendition = arendition;
					}
				}
			}
			else
			{
				if(!rendition || (rendition->rendition_ext_info_ && arendition->rendition_ext_info_ && rendition->rendition_ext_info_->rendition_used_ && !arendition->rendition_ext_info_->rendition_used_))
				{
					rendition = arendition;
				}
			}
		}

		if(candidate->ad_ && candidate->ad_->faked_ == true)
		{
			if(rendition && rendition->rendition_ext_info_ && rendition->rendition_ext_info_->rendition_used_)
				return 0;
		}
		if (rendition) return rendition;
		if (!vast_renditions.empty()) return vast_renditions.front();
		if (!renditions.empty()) return renditions.front();

		return 0;
	}

	int Ads_Selector::load_and_apply_restrictions(Ads_Selection_Context *ctx, ads::MEDIA_TYPE assoc)
	{
		if (ctx == nullptr || ctx->repository_ == nullptr)
		{	
			ADS_LOG((LP_ERROR, "ctx[%p] or repo is nullptr.\n", ctx));
			return -1;
		}
		if (!ctx->asset(assoc))
			return -1;

		const Ads_Asset_Base *dummy_root_asset = nullptr;
		Ads_GUID_List dummy_asset_dist_path, dummy_asset_dist_mirrors, dummy_asset_content_right_path, dummy_asset_content_right_mirrors;
		find_asset_section_distribution_content_right_path(ctx, assoc, ctx->asset(assoc), dummy_root_asset, dummy_asset_dist_path,
				dummy_asset_dist_mirrors, dummy_asset_content_right_path, dummy_asset_content_right_mirrors, true/*need apply restriction*/);

		if (ctx->scheduler_->is_programmer_schedule() && (assoc == ads::MEDIA_VIDEO))
		{
			load_and_apply_break_restrictions(*ctx, *(ctx->repository_), ctx->scheduler_->cro_network_id_, ctx->scheduler_->break_id_, ctx->restriction(ads::MEDIA_VIDEO));
		}
		return 0;
	}

	int Ads_Selector::load_and_apply_break_restrictions(Ads_Selection_Context &ctx, const Ads_Repository &repo, Ads_GUID network_id, Ads_GUID break_id, selection::Restriction &restriction)
	{
		Ads_Asset_Restriction *break_restriction = nullptr;
		Ads_Restriction_Controller::get_break_restriction(ctx, repo, network_id, break_id, ctx.restrictions_, break_restriction);
		if (break_restriction == nullptr)
			return -1;
		Ads_Restriction_Controller::merge_break_restriction(*break_restriction, network_id, restriction);
		return 0;
	}

	bool Ads_Selector::test_advertisement_rating(Ads_Selection_Context* ctx, const Ads_Advertisement* ad)
	{
		// TODO(pfliu): Consider if this function should be moved to Ads_Advertisement_Candidate_Lite
		ADS_ASSERT(ctx && ad);
		if (!Ads_Ratable::match(ctx->restriction_.rating_.first, ctx->restriction_.rating_.second, ad->rating_type_, ad->rating_value_))
		{
			ADS_DEBUG((LP_TRACE, "%s (%d, %d) failed for (%d, %d)\n", ad->get_ad_type_and_id().c_str() , ad->rating_type_, ad->rating_value_, ctx->restriction_.rating_.first, ctx->restriction_.rating_.second));
			return false;
		}
		return true;
	}

void
Ads_MRM_Rule_Path::dump() const
{
	ADS_DEBUG0((LP_TRACE, "path to %s: ", ADS_ENTITY_ID_CSTR(this->terminal_partner_id())));
	for (const auto &edge : this->edges_)
		ADS_DEBUG0((LP_TRACE, "%s->", ADS_ENTITY_ID_CSTR(edge.partner_id())));
	ADS_DEBUG0((LP_TRACE, ";"));

	for (const auto &edge : this->edges_)
		ADS_DEBUG0((LP_TRACE, "%s_%s->", ADS_ENTITY_ID_CSTR(edge.rule_id_), ADS_ENTITY_ID_CSTR(edge.rule_ext_id_)));
	ADS_DEBUG0((LP_TRACE, ";"));

	for (const auto &edge : this->edges_)
	{
		ADS_DEBUG0((LP_TRACE, "%s->(%s)%s|", ADS_ASSET_ID_CSTR(edge.raw_edge_.asset_id_),
							ADS_ENTITY_ID_CSTR(edge.partner_id()), ADS_ASSET_ID_CSTR(edge.raw_edge_.mirror_asset_id_)));
	}
	ADS_DEBUG0((LP_TRACE, ";"));

	for (const auto &edge : this->edges_)
	{
		const auto &rsf = edge.revenue_share_factor_;
		ADS_DEBUG0((LP_TRACE, "(%f,%f),", ADS_TO_REAL_PERCENTAGE(rsf.first), ADS_TO_REAL_CURRENCY(rsf.second) * 1000.0));
	}
	ADS_DEBUG0((LP_TRACE, "\n"));
}

/* the minimal single pixel transparent GIF, 43 bytes */
static unsigned char empty_gif[] =
{
	'G', 'I', 'F', '8', '9', 'a',  /* header                                 */

	/* logical screen descriptor              */
	0x01, 0x00,                    /* logical screen width                   */
	0x01, 0x00,                    /* logical screen height                  */
	0x80,                          /* global 1-bit color table               */
	0x01,                          /* background color #1                    */
	0x00,                          /* no aspect ratio                        */

	/* global color table                     */
	0x00, 0x00, 0x00,              /* #0: black                              */
	0xff, 0xff, 0xff,              /* #1: white                              */

	/* graphic control extension              */
	0x21,                          /* extension introducer                   */
	0xf9,                          /* graphic control label                  */
	0x04,                          /* block size                             */
	0x01,                          /* transparent color is given,            */
	/*     no disposal specified,             */
	/*     user input is not expected         */
	0x00, 0x00,                    /* delay time                             */
	0x01,                          /* transparent color #1                   */
	0x00,                          /* block terminator                       */

	/* image descriptor                       */
	0x2c,                          /* image separator                        */
	0x00, 0x00,                    /* image left position                    */
	0x00, 0x00,                    /* image top position                     */
	0x01, 0x00,                    /* image width                            */
	0x01, 0x00,                    /* image height                           */
	0x00,                          /* no local color table, no interlaced    */

	/* table based image data                 */
	0x02,                          /* LZW minimum code size,                 */
	/*     must be at least 2-bit             */
	0x02,                          /* block size                             */
	0x4c, 0x01,                    /* compressed bytes 01_001_100, 0000000_1 */
	/* 100: clear code                        */
	/* 001: 1                                 */
	/* 101: end of information code           */
	0x00,                          /* block terminator                       */

	0x3B                           /* trailer                                */
};

int
Ads_Selector::dot(const Ads_Repository *repo, Ads_Selection_Context *ctx, Ads_Request &req, Ads_Response &res)
{
	Ads_Slot_List_Guard tmp_video_slots(new Ads_Slot_List());
	Ads_Slot_List_Guard tmp_page_slots(new Ads_Slot_List());
	Ads_Slot_List_Guard tmp_player_slots(new Ads_Slot_List());
	Ads_Slot_Guard tmp_dot_slot(nullptr);

	if (ctx == nullptr || ctx->is_no_slot())
	{
		if (req.rep() == nullptr)
			ADS_ERROR_RETURN((LP_ERROR, "null ctx and null req rep, no slot found\n"), 0);

		Ads_Slot_Controller slot_controller;
		ads::Error_List dummy_errors;
		slot_controller.initialize(*repo, *req.rep(), dummy_errors);
		slot_controller.generate_display_slots(false, tmp_page_slots.value(), tmp_player_slots.value(), tmp_dot_slot.value());
		slot_controller.generate_video_slots_by_request_rep(tmp_video_slots.value(), nullptr);
		Ads_Monitor::stats_inc(Ads_Monitor::DOT_SLOT_FALLBACK);
	}

	Ads_Slot_List &video_slots = (tmp_video_slots.empty() && ctx != nullptr) ? ctx->video_slots_ : tmp_video_slots.value();
	Ads_Slot_List &page_slots = (tmp_page_slots.empty() && ctx != nullptr) ? ctx->page_slots_ : tmp_page_slots.value();
	Ads_Slot_List &player_slots = (tmp_player_slots.empty() && ctx != nullptr) ? ctx->player_slots_ : tmp_player_slots.value();
	Ads_Slot_Base *dot_slot = (tmp_dot_slot.empty() && ctx != nullptr) ? ctx->dot_slot_ : tmp_dot_slot.value();

	res.user(req.user());
	res.initialize(&req);

	if (res.format() == Ads_Response::LEGACY)
	{
		Ads_Slot_Base *slot = 0;
		if (!video_slots.empty()) slot = video_slots.front();
		else if (!player_slots.empty()) slot = player_slots.front();
		else if (!page_slots.empty()) slot = page_slots.front();
		else slot = dot_slot;

		if (!slot)
		{
			ADS_ERROR_RETURN((LP_ERROR, "no slot found\n"), 0);
		}

		Ads_GUID_Set content_types;
		if (!slot->primary_content_types_.empty())
			content_types.insert(slot->primary_content_types_.begin(), slot->primary_content_types_.end());
		else if (slot->profile_)
		{
			const Ads_Environment_Profile * profile = slot->profile_;
			if (profile->type_ == Ads_Environment_Profile::COMPOUND && !profile->sub_profiles_.empty())
				profile = *profile->sub_profiles_.begin();
			content_types.insert(profile->primary_content_types_.begin(), profile->primary_content_types_.end());
		}

		Ads_String redirect;
		Ads_String output;
		Ads_String mime_type;

		Ads_Macro_Environment menv;
		menv.repo_ = repo;
		menv.slot_ = slot;
		menv.request_ = &req;

		bool has_dot_url = false;
		for (Ads_GUID_Set::const_iterator it = content_types.begin(); it != content_types.end(); ++it)
		{
			const Ads_Content_Type *ct = 0;
			if (repo->find_content_type(*it, ct) < 0 || !ct)
				continue;

			if (!ct->dot_content_.empty())
			{
				output = ct->dot_content_.c_str();
				mime_type = ct->mime_type_.c_str();
				has_dot_url = true;
				break;
			}

			if (ct->dot_url_.empty())
				continue;

			redirect = ct->dot_url_.c_str();
			ADS_ASSERT(redirect[0] != '/');
			has_dot_url = true;
			break;
		}

		if (!has_dot_url)
		{
			int preference = -1;

			const Ads_Creative_Rendition_Map& dot_creatives = repo->dot_creatives();
			for (Ads_Creative_Rendition_Map::const_iterator it = dot_creatives.begin(); it != dot_creatives.end(); ++it)
			{
				const Ads_Creative_Rendition *creative = it->second;

				ADS_ASSERT(creative);
				if (!creative) continue;

				if (creative->preference_ <= preference) continue;

				const Ads_Creative_Rendition_Asset *ra = creative->primary_asset_;
				if (!ra) continue;

				menv.creative_ = creative;

				const Ads_Content_Type_Implicity *implicity = 0;
				if (!repo->compatible(ra->content_type_id_, content_types.begin(), content_types.end())
				    && repo->implies(ra->content_type_id_, content_types.begin(), content_types.end(), implicity) < 0)
					continue;

				if (!ra->content_.empty())
				{
					if (Ads_Macro_Expander::instance()->translate(&menv, ra->content_.c_str(), output, false) < 0)
					{
						ADS_DEBUG((LP_DEBUG, "failed to translate content %s\n", ra->content_.c_str()));
						continue;
					}
				}
				else
				{
					ADS_ASSERT(!ra->location_.empty());

					Ads_String location = ra->location_.c_str();
					if (location[0] == '/') location = "#{server.plainMediaHost}" + location;

					Ads_String url;
					if (ra->location_.empty() || Ads_Macro_Expander::instance()->translate(&menv, location, url, false) < 0 || url.empty())
					{
						ADS_DEBUG((LP_DEBUG, "failed to translate rendition url %s\n", location.c_str()));
						continue;
					}

					redirect = url;
				}

				mime_type = ra->mime_type_.c_str();
				if (!implicity || implicity->path_.empty())
				{
					preference = creative->preference_;
					continue;
				}

				const Ads_String& src = !redirect.empty()? redirect : output;
				Ads_String result;
				std::list<size_t> path(implicity->path_.begin(), implicity->path_.end());
				if (Ads_Macro_Expander::instance()->transform(&menv, src, result, path) < 0)
				{
					ADS_DEBUG((LP_DEBUG, "failed to transform %s -> profile %s\n", src.c_str(), slot->profile_->name_.c_str()));
					continue;
				}

				const Ads_Content_Type *ct = 0;
				if (repo->find_content_type(implicity->target_, ct) < 0 || !ct)
					continue;

				mime_type = ct->mime_type_.c_str();
				redirect.clear();
				output.swap(result);
				preference = creative->preference_;
			}
		}

		if (!redirect.empty())
			res.redirect(redirect);
		else if (!output.empty())
			res.content(mime_type, output);

		res.ready(true);
	}
	res.add_custom_header("X-FW-Power-By", "Dot");

	return 0;
}

int
Ads_Selector::open()
{
	selection::External_Ad_Connection_Manager::instance()->open();
	return 0;
}

bool
Ads_Selector::ptiling(const Ads_Repository &repo, Ads_Request &req, Ads_Response &res)
{
	if (req.rep() == nullptr)
		return false;

	Ads_Slot_List_Guard page_slots(new Ads_Slot_List());
	Ads_Slot_Controller slot_controller;
	ads::Error_List errors; // Temporary used. Won't be logged in res.
	slot_controller.initialize(repo, *req.rep(), errors);
	slot_controller.generate_page_slots_for_ptiling(page_slots.value());

	if (!is_ptiling_request(req, page_slots.value()))
		return false;

	const Ads_Slot_Base *slot = page_slots.empty() ? nullptr : page_slots.value().front();
	if (!slot || !slot->profile_ || slot->profile_->name_ != "g_js")
		return false;

	if (initiate_ptiling(repo, page_slots.value(), req, res) >= 0)
	{
		req.audit_checker_.set_filtration_reason(Ads_Audit::FILTERED_BY_PTILING, false);
		log_banned_request(&req, repo, &page_slots.value());
		return true;
	}
	else
	{
		ADS_LOG((LP_ERROR, "failed to initiate ptiling: %s\n", req.uri().c_str()));
		if (res.rep())
		{
			delete res.rep(); res.rep(0);
		}
	}
	return false;
}

bool
Ads_Selector::is_ptiling_request(const Ads_Request &req, const Ads_Slot_List &page_slots) const
{
	return req.method_ == Ads_Request::GET && req.rep() != nullptr && req.rep()->response_format_ == "ad" && req.rep()->capabilities_.ptiling_ == 1 && req.rep()->events_.empty() && !page_slots.empty();
}

int
Ads_Selector::initiate_ptiling(const Ads_Repository &repo, const Ads_Slot_List &page_slots, Ads_Request &req, Ads_Response &res)
{
	res.user(req.user());
	res.initialize(&req);

	const char *ptiling_initiator = repo.system_variable("ptiling_initiator");
	if (!ptiling_initiator)
	{
		ADS_LOG((LP_ERROR, "template for ptiling_initiator not found\n"));
		++this->stats_.n_ptiling_failed_;
		Ads_Monitor::stats_inc(Ads_Monitor::PTILING_FAILED);
		return -1;
	}

	Ads_Slot_Base *slot = 0;
	if (!page_slots.empty()) slot = page_slots.front();
	ADS_ASSERT(slot);

	/// redundant check
	if (!slot || !slot->profile_ || slot->profile_->name_ != "g_js") return -1;

	Ads_Macro_Environment menv;
	menv.repo_ = &repo;
	menv.request_ = &req;
	menv.slot_ = slot;

	Ads_String output;
	if (Ads_Macro_Expander::instance()->translate(&menv, ptiling_initiator, output, false) < 0 || output.empty())
	{
		ADS_LOG((LP_ERROR, "failed to translate ptiling_initiator \n"));
		++this->stats_.n_ptiling_failed_;
		Ads_Monitor::stats_inc(Ads_Monitor::PTILING_FAILED);
		return -1;
	}

	++this->stats_.n_ptiling_;
	Ads_Monitor::stats_inc(Ads_Monitor::PTILING_TOTAL);
	Ads_String mime_type = "text/javascript";
	res.content(mime_type, output);
	res.ready(true);

//	res->add_custom_header("X-FW-Power-By: Ptiling\r\n");
	return 0;
}

int
Ads_Selector::translate_redirect_url(const Ads_Repository::System_Config &config, const Ads_String &user_id, const Ads_String &redirect_url, Ads_String &output_url, Ads_String &error_msg)
{
	Ads_String dummy_domain;
	Ads_String base_domain;
	if (Ads_Server::extract_domain(redirect_url, dummy_domain, base_domain) < 0 || base_domain.empty())
	{
		ADS_LOG((LP_ERROR, "ad/u: failed to extract base domain\n"));
		error_msg.assign("Not a valid domain or redirect url is not percentage encoded!");
		return -1;
	}
	if (!config.is_domain_allowed_for_user_exchange(base_domain.c_str()))
	{
		ADS_LOG((LP_ERROR, "ad/u: domain[%s] is not allowed to map with fw user id\n", base_domain.c_str()));
		error_msg.assign("Domain " + base_domain + " is not allowed for Freewheel to sync user id with it. Please contact your Freewheel Solution Engineer!");
		return -1;
	}
	Ads_Macro_Environment menv;
	menv.user_id_ = user_id;
	Ads_Macro_Expander::instance()->translate(&menv, redirect_url, output_url, false);
	return 0;
}

bool
Ads_Selector::check_sync_url_validation(const Ads_Request &req, Ads_String &error_msg)
{
	Ads_GUID data_provider_id = ads::str_to_i64(req.p(FW_DATA_PROVIDER_ID).c_str());
	if (!ads::entity::is_valid_id(data_provider_id))
	{
		ADS_LOG((LP_ERROR, "ad/u: invalid data_provider_id[%llu], specify with dpid=\n", data_provider_id));
		error_msg.assign("Invalid data provider id specified with dpid=");
		return false;
	}
	const Ads_String &token = req.p(FW_REQUEST_TOKEN);
	if (token.empty())
	{
		ADS_LOG((LP_ERROR, "ad/u: invalid token\n"));
		error_msg.assign("Invalid token specified with token=");
		return false;
	}
	return true;
}

bool
Ads_Selector::is_exchanged_user_filtered(bool is_anonymous_user, bool is_first_visit_user, const Ads_String &user_id)
{
	if (is_anonymous_user)
	{
		Ads_Monitor::stats_inc(Ads_Monitor::USER_AD_U_ANONYMOUS);
		ADS_DEBUG((LP_DEBUG, "ad/u: ignore user exchange for anonymous user id: [%s]\n", user_id.c_str()));
		return true;
	}
	if (is_first_visit_user)
	{
		Ads_Monitor::stats_inc(Ads_Monitor::USER_AD_U_EXP);
		ADS_DEBUG((LP_DEBUG, "ad/u: not allowed to map with first visit fw user id[%s]\n", user_id.c_str()));
		return true;
	}
	return false;
}

int
Ads_Selector::post_user_log_msg(const Ads_String &user_id, const Ads_String &external_user_id, const Ads_String &attributes, const Ads_String &publisher_user_id,
	const Ads_String &data_provider_id, const Ads_String &network_id)
{
	ADS_ASSERT(!user_id.empty());
	if (user_id.empty())
	{
		ADS_LOG((LP_ERROR, "ad/u: fw user id is empty\n"));
		return -1;
	}
	// specific parameter "nw" in ad/u url means that user info from related data provider(dpid) can only be seen by this specific network.
	// otherwise, if there is no specific "nw", user info from related data provider can be seen by all networks.
	Ads_GUID nwid = ads::entity::make_id(ADS_ENTITY_TYPE_NETWORK, ads::str_to_i64(network_id.c_str()));
	if (!ads::entity::is_valid_id(nwid))
	{
		nwid = ads::entity::make_id(ADS_ENTITY_TYPE_NETWORK, 0);
	}
	Ads_GUID dpid = ads::entity::make_id(ADS_ENTITY_TYPE_NETWORK, ads::str_to_i64(data_provider_id.c_str()));
	auto r = new Ads_User_Log_Record(user_id, dpid, nwid, Ads_Server_Config::instance()->id_, Ads_User_Log_Record::URL, ::time(NULL));

	if (!attributes.empty())
	{
		// monitor test: "attr", "buid", "puid" should not appear simultaneously
		if (!external_user_id.empty() || !publisher_user_id.empty())
		{
			Ads_Monitor::stats_inc(Ads_Monitor::USER_AD_U_SYNC_INVALID_PARAM);
		}
		// <fw_user_id, attr> -> data ingest against freewheel user id
		Ads_Monitor::stats_inc(Ads_Monitor::USER_AD_U_SYNC_ATTRIBUTES_TOTAL);
		r->record_type_ = ::audience::User_Log_Record_Record_Type_KEY_VALUE;
		ads::split(attributes, r->user_attributes_, ',');
	}
	else if (!external_user_id.empty())
	{
		// monitor test: "buid", "puid" should not appear simultaneously
		if (!publisher_user_id.empty())
		{
			Ads_Monitor::stats_inc(Ads_Monitor::USER_AD_U_SYNC_INVALID_PARAM);
		}
		// <fw_user_id, buid> -> normal user mapping
		Ads_Monitor::stats_inc(Ads_Monitor::USER_AD_U_SYNC_MAPPING_TOTAL);
		r->record_type_ = ::audience::User_Log_Record_Record_Type_MAPPING;
		r->custom_user_id_ = external_user_id;
	}
	else if (!publisher_user_id.empty())
	{
		// <fw_user_id, puid> -> internal user mapping
		Ads_Monitor::stats_inc(Ads_Monitor::USER_AD_U_SYNC_INTERNAL_MAPPING_TOTAL);
		r->record_type_ = ::audience::User_Log_Record_Record_Type_INTERNAL_MAPPING;
		r->custom_user_id_ = publisher_user_id;
	}
	else
	{
		r->destroy();
		ADS_LOG((LP_ERROR, "ad/u: need to specify 'buid=' or 'attr=' or 'puid='\n"));
		return -1;
	}

	Ads_Message_Base *msg = Ads_Message_Base::create(Ads_Logger_Message::MESSAGE_USER_LOG_RECORD, (void *)r);
	if (Ads_Log_Service::instance()->post_message(msg, Ads_Message_Base::PRIORITY_IDLE) < 0)
	{
		r->destroy();
		msg->destroy();
		ADS_LOG((LP_ERROR, "ad/u: failed to post user info msg\n"));
		return -1;
	}
	return 0;
}

int
Ads_Selector::exchange_user(const Ads_Repository *repo, Ads_Request *req, Ads_Response *res)
{
	if (repo == nullptr || req == nullptr || res == nullptr)
	{
		ADS_LOG((LP_ERROR, "ad/u: repo[%p] or req[%p] or res[%p] is nullptr\n",
			repo, req, res));
		return -1;
	}

	Ads_Monitor::stats_inc(Ads_Monitor::USER_AD_U_TOTAL);

	// initialize response
	res->user(req->user());
	res->initialize(req);
	// monitor: use vcid2 as fw user id.
	Ads_String is_dsp_user_mapping = req->p("dsp_user_mapping");
	if (req->id_repo_.server_user_id().is_valid())
	{
		Ads_Monitor::stats_inc(Ads_Monitor::USER_AD_U_VCID2_AS_FW_ID);
	}
	if (req->gdpr_consent_controller_.is_hybrid_controller_no_purpose_1())
	{
		Ads_Monitor::stats_inc(Ads_Monitor::USER_AD_U_DISABLED_BY_GDPR);
		ADS_DEBUG((LP_DEBUG, "ad/u: ignore user exchange for GDPR no controller purpose 1. \n"));
		res->ready(true);
		return 0;
	}
	Ads_String error_msg;
	const Ads_String &exchange_mode = req->p(FW_REQUEST_MODE);
	bool is_anonymous_user = (req->user() != nullptr && req->user()->anonymous());
	if (exchange_mode == "echo")
	{
		Ads_Monitor::stats_inc(Ads_Monitor::USER_AD_U_ECHO_TOTAL);
		if (is_exchanged_user_filtered(is_anonymous_user, req->id_repo_.is_first_visit(), req->id_repo_.user_id().id()))
		{
			res->ready(true);
			return 0;
		}
		Ads_String output_url;
		const Ads_String &redirect_url = req->p(FW_CALLBACK_REDIRECT);
		int ret = translate_redirect_url(repo->system_config(), req->id_repo_.user_id().id(), redirect_url, output_url, error_msg);

		if (ret < 0)
		{
			Ads_Monitor::stats_inc(Ads_Monitor::USER_AD_U_ECHO_FAILED);
			res->status_code(403);
			res->content("text/html", error_msg);
			return -1;
		}
		res->redirect(output_url);
		res->ready(true);
		return 0;
	}
	else if (exchange_mode == "dsp_user_mapping")
	{
		if (is_exchanged_user_filtered(is_anonymous_user, req->id_repo_.is_first_visit(), req->id_repo_.user_id().id()))
		{
			res->ready(true);
			return 0;
		}
		Ads_String output_url;
		const Ads_String &redirect_url = req->p(FW_CALLBACK_REDIRECT);
		int ret = translate_redirect_url(repo->system_config(), req->id_repo_.user_id().id(), redirect_url, output_url, error_msg);
		if (ret < 0)
		{
			res->status_code(403);
			res->content("text/html", error_msg);
			return -1;
		}
		res->redirect(output_url);
		res->ready(true);
		return 0;
	}
	else if (ads::tolower(is_dsp_user_mapping) == "true")// dsp user mapping
	{
		Ads_Monitor::stats_inc(Ads_Monitor::EX_DSP_USER_SYNC_TOTAL);
		bool is_server_side_sync = !req->p("mrmuid").empty();
		Ads_String user_id = is_server_side_sync ? req->p("mrmuid") : req->id_repo_.user_id().id();
		if (is_server_side_sync)
		{
			Ads_Monitor::stats_inc(Ads_Monitor::EX_DSP_USER_SYNC_SERVER_SIDE_TOTAL);
		}
		else
		{
			const Ads_String redirect_url = req->p("rdU");
			Ads_String output_url;
			Ads_Macro_Environment menv;
			menv.user_id_ = user_id;
			if (redirect_url.empty() || Ads_Macro_Expander::instance()->translate(&menv, redirect_url, output_url, false) < 0)
			{
				Ads_Monitor::stats_inc(Ads_Monitor::EX_DSP_USER_SYNC_INVALID_REDIRECT_URL);
				ADS_LOG((LP_ERROR, "dsp user mapping ad/u: failed echo sync, rdU is %s\n", redirect_url.c_str()));
			}
			res->redirect(output_url);
			if (is_exchanged_user_filtered(is_anonymous_user, req->id_repo_.is_first_visit(), user_id))
			{
				res->ready(true);
				return 0;
			}
		}

		for (auto& parameter : req->parameters_)
		{
			if (!ads::is_numeric(parameter.first))
				continue;

			const Ads_String &third_party_id = parameter.first;
			const Ads_String &third_party_user_id = parameter.second;

			if (selection::Auction_Manager::record_user_mapping(repo, user_id, third_party_id, third_party_user_id) < 0)
			{
				ADS_LOG((LP_ERROR, "dsp user mapping ad/u: failed sync uri is %s\n", req->uri().c_str()));
			}
		}
		res->ready(true);
		return 0;
	}
	else //sync mode
	{
		Ads_Monitor::stats_inc(Ads_Monitor::USER_AD_U_SYNC_TOTAL);
		if (!check_sync_url_validation(*req, error_msg))
		{
			Ads_Monitor::stats_inc(Ads_Monitor::USER_AD_U_SYNC_FAILED);
			res->status_code(403);
			res->content("text/html", error_msg);
			return -1;
		}
		bool expect_dot_gif = (req->p(FW_EXPECT_DOT_GIF) == "1");
		if (expect_dot_gif)
		{
			res->content("image/gif", Ads_String((const char *)empty_gif, sizeof(empty_gif)));
		}
		if (is_exchanged_user_filtered(is_anonymous_user, req->id_repo_.is_first_visit(), req->id_repo_.user_id().id()))
		{
			res->ready(true);
			return 0;
		}
		if (post_user_log_msg(req->id_repo_.user_id().id(), req->p(FW_USER_CUSTOM_ID), req->p(FW_USER_ATTRIBUTES), req->p(FW_PUBLISHER_USER_ID),
			req->p(FW_DATA_PROVIDER_ID), req->p(FW_NETWORK_ID)) < 0)
		{
			Ads_Monitor::stats_inc(Ads_Monitor::USER_AD_U_SYNC_FAILED);
			ADS_LOG((LP_ERROR, "ad/u: failed sync uri is %s\n", req->uri().c_str()));
			res->status_code(403);
			res->content("text/html", "Please check sync url!");
			return -1;
		}
		res->ready(true);
		return 0;
	}
}

int
Ads_Selector::opt_out(const Ads_Repository &repo, const Ads_Request &req, Ads_Response *res) const
{
	if (res == nullptr)
	{
		ADS_LOG((LP_ERROR, "opt_out: res is nullptr\n", res));
		return -1;
	}
	Ads_Monitor::stats_inc(Ads_Monitor::TX_OPTOUT_REQUEST);
	res->need_samesite_ = req.need_samesite();
	const Ads_String& domain = req.domain();
	Ads_String now = Ads_Server::httpdate(0);
	if (req.p(FW_OPTOUT_REVOKE) == "true")
	{
		Ads_Monitor::stats_inc(Ads_Monitor::TX_OPTOUT_REVOKE);
		res->add_cookie(domain, now, FW_COOKIE_OPT_OUT, "false");
	}
	else
	{
		if (req.p(FW_OPTOUT_CLEAN) != "true")
		{
			Ads_Monitor::stats_inc(Ads_Monitor::TX_OPTOUT_SET);
			Ads_String expires = Ads_Server::httpdate(::time(NULL) + 3600 * 24 * 365 * 10); //10 years
			res->add_cookie(domain, expires, FW_COOKIE_OPT_OUT, "true");
		}

		for(const auto& cookie : req.cookies())
		{
			const Ads_String& cookie_key = cookie.first;
			res->add_cookie(domain, now, cookie_key, "");
		}
		// Clean up user id cookie: _uid.
		res->add_cookie(domain, now, FW_COOKIE_USER_ID, "");
	}

	const Ads_String &callback_redirect = req.p(FW_CALLBACK_REDIRECT);
	if (!callback_redirect.empty())
	{
		Ads_Monitor::stats_inc(Ads_Monitor::TX_OPTOUT_CALLBACK_REDIRECT);
		res->redirect(callback_redirect);
	}
	else
	{
		const char *opt_out_redirect = repo.system_variable("opt_out_redirect");
		if (opt_out_redirect && opt_out_redirect[0])
			res->redirect(Ads_String(opt_out_redirect));
	}

	return 0;
}

int
Ads_Selector::get_daa_opt_out_cookie_status(const Ads_Request& req, Ads_Response *res) const
{
	if (res == nullptr)
	{
		ADS_LOG((LP_ERROR, "get_daa_opt_out_cookie_status: nullptr res.\n"));
		return -1;
	}

	const Ads_String COOKIE_STATUS_NO_OBA_COOKIE = "1";         //No FreeWheel OBA cookie(s) exists in the browser
	const Ads_String COOKIE_STATUS_HAVE_OBA_COOKIE = "2";       //FreeWheel OBA cookie(s) exists in the browser
	const Ads_String COOKIE_STATUS_HAVE_DAAOPTOUT_COOKIE = "3"; //FreeWheel DAA opt-out cookie exists in the browser

	const Ads_String& optout_cookie = req.c(FW_COOKIE_DAA_OPT_OUT);
	const Ads_String& participant_id = req.p(FW_DAAOPTOUT_PARTICIPANT_ID);
	ADS_DEBUG((LP_DEBUG, "get_daa_opt_out_cookie_status, optout_cookie: %s\n", optout_cookie.c_str()));

	Ads_String rd = req.p(FW_DAAOPTOUT_REDIRECT);
	Ads_Request::decode_url(rd);

	char token_cstr[0xF] = { 0 };
	time_t epoch_now = ::time(NULL);
	sprintf(token_cstr, "%d", (int) epoch_now);
	const Ads_String token(token_cstr);

	Ads_String status;
	if (optout_cookie.empty())
	{
		bool oba_cookie_exists = (req.user() && req.user()->anonymous()) ? false: true;
		if (!oba_cookie_exists)
		{
			Ads_Monitor::stats_inc(Ads_Monitor::TX_DAA_OPTOUT_NO_OBA_COOKIE);
			ADS_DEBUG((LP_DEBUG, "no OBA cookie(s) on the browser\n"));
			status = COOKIE_STATUS_NO_OBA_COOKIE;
		}
		else
		{
			Ads_Monitor::stats_inc(Ads_Monitor::TX_DAA_OPTOUT_OBA_COOKIE);
			ADS_DEBUG((LP_DEBUG, "OBA cookie(s) present on the browser\n"));
			status = COOKIE_STATUS_HAVE_OBA_COOKIE;
		}
	}
	else if (optout_cookie == "true")
	{
		Ads_Monitor::stats_inc(Ads_Monitor::TX_DAA_OPTOUT_COOKIE_PRESENT_SUCCESS);
		ADS_DEBUG((LP_DEBUG, "Opt out cookie is present on the browser\n"));
		status = COOKIE_STATUS_HAVE_DAAOPTOUT_COOKIE;
	}
	else
	{
		Ads_Monitor::stats_inc(Ads_Monitor::TX_DAA_OPTOUT_COOKIE_PRESENT_WRONG);
		/// If the daa optout cookie is present on the browser,
		/// but the value is not true, we also return status with "status = 1".
		status = COOKIE_STATUS_NO_OBA_COOKIE;
	}

	Ads_String redirect = rd + "/token/" + participant_id + "/" + status + "/" + token; //Eg. http://www.aboutads.info/token/123456/2/1419304741
	res->redirect(redirect);
	return 0;
}

int
Ads_Selector::set_daa_opt_out_cookie(const Ads_Request& req, Ads_Response *res) const
{
	if (res == nullptr)
	{
		ADS_LOG((LP_ERROR, "set_daa_opt_out_cookie: nullptr res.\n"));
		return -1;
	}

	const Ads_String SET_DAAOPTOUT_COOKIE_SUCCESS = "1";       //AdServer set the Freewheel DAA opt-out cookie in end-user's browser successfully
	const Ads_String SET_DAAOPTOUT_COOKIE_FAILED = "2";        //AdServer failed to set the Freewheel DAA opt-out cookie in end-user's browser
	const Ads_String SET_DAAOPTOUT_COOKIE_INVALID_TOKEN = "3"; //anti-CSRF token not match or missing

	const Ads_String& optout_cookie = req.c(FW_COOKIE_DAA_OPT_OUT);
	const Ads_String& token = req.p(FW_DAAOPTOUT_TOKEN);
	const Ads_String& checktag = req.p(FW_DAAOPTOUT_CHECKTAG);
	const Ads_String& participant_id = req.p(FW_DAAOPTOUT_PARTICIPANT_ID);
	const Ads_String& domain = req.domain();
	ADS_DEBUG((LP_DEBUG, "set_daa_opt_out_cookie, optout_cookie: %s\n", optout_cookie.c_str()));

	Ads_String rd = req.p(FW_DAAOPTOUT_REDIRECT);
	Ads_Request::decode_url(rd);

	Ads_String result_id;
	if (token.empty())
	{
		Ads_Monitor::stats_inc(Ads_Monitor::TX_DAA_OPTOUT_TOKEN_MISSING);
		ADS_DEBUG((LP_DEBUG, "opt out failed: anti-CSRF token missing\n"));
		result_id = SET_DAAOPTOUT_COOKIE_INVALID_TOKEN;
	}
	else
	{
		int epoch_token = ::atoi(token.c_str());
		time_t epoch_now = ::time(NULL);
		if (epoch_now - epoch_token > DAA_OPTOUT_TOKEN_TTL) // 1 hour
		{
			Ads_Monitor::stats_inc(Ads_Monitor::TX_DAA_OPTOUT_TOKEN_MISMATCH);
			ADS_DEBUG((LP_DEBUG, "opt out failed: anti-CSRF token mismatch\n"));
			result_id = SET_DAAOPTOUT_COOKIE_INVALID_TOKEN;
		}
		else if (checktag.empty())// Set DAA opt-out cookie and redirect to check if DAA opt-out cookie exists
		{
			Ads_Monitor::stats_inc(Ads_Monitor::TX_DAA_OPTOUT_CHECKTAG_EMPTY);
			ADS_DEBUG((LP_DEBUG, "checktag is empty, set cookie and redirect to self.\n"));
			const Ads_String now = Ads_Server::httpdate(0);
			const Ads_String expires = Ads_Server::httpdate(::time(NULL) + 3600 * 24 * 365 * 5); //5 years

			res->add_cookie(domain, expires, FW_COOKIE_OPT_OUT, "true");
			res->add_cookie(domain, expires, FW_COOKIE_DAA_OPT_OUT, "true");
			for(const auto& cookie : req.cookies())
			{
				const Ads_String& cookie_key = cookie.first;
				res->add_cookie(domain, now, cookie_key, "");
			}
			// Clean up user id cookie: _uid.
			res->add_cookie(domain, now, FW_COOKIE_USER_ID, "");

			//Eg. ad/daa_optout?participant_id=123456&action_id=4&token=1893427200&rd=http%3A%2F%2F127.0.0.1:55557&token=LUATOKEN&checktag=1
			const Ads_String redirect_to_self = req.uri() + "&" FW_DAAOPTOUT_CHECKTAG "=1";
			res->redirect(redirect_to_self);
			return 0;
		}
		else if (checktag == "1") // When "checktag=1", AdServer would check if Freewheel DAA opt-out cookie exists in the request header
		{
			if (optout_cookie == "true")
			{
				Ads_Monitor::stats_inc(Ads_Monitor::TX_DAA_OPTOUT_SUCCESS);
				ADS_DEBUG((LP_DEBUG, "success: after an opt out attempt, the opt out cookie is present on the user's browser.\n"));
				result_id = SET_DAAOPTOUT_COOKIE_SUCCESS;
			}
			else
			{
				if (optout_cookie.empty())
					Ads_Monitor::stats_inc(Ads_Monitor::TX_DAA_OPTOUT_FAILED_EMPTY_COOKIE);
				else
					Ads_Monitor::stats_inc(Ads_Monitor::TX_DAA_OPTOUT_FAILED_WRONG_COOKIE);
				ADS_DEBUG((LP_DEBUG, "failed: after an opt out attempt, the cookie is not present on a user's browser or has wrong value.\n"));
				result_id = SET_DAAOPTOUT_COOKIE_FAILED;
			}
		}
	}

	const Ads_String redirect = rd + "/finish/" + participant_id + "/4/" + result_id + "/OK/"; //Eg. http://127.0.0.1:55557/finish/123456/4/2/OK/
	res->redirect(redirect);

	return 0;
}

int
Ads_Selector::daa_opt_out(const Ads_Repository &repo, const Ads_Request& req, Ads_Response *res) const
{
	if (res == nullptr)
	{
		ADS_LOG((LP_ERROR, "daa_opt_out: res is nullptr\n"));
		return -1;
	}

	static const Ads_String DAA_PARICIPANT_INFO = "http://www.fwmrm.net/daa/paricipant_info";
	Ads_Monitor::stats_inc(Ads_Monitor::TX_DAA_OPTOUT_REQUEST);
	res->need_samesite_ = req.need_samesite();
	const Ads_String& action_id = req.p(FW_DAAOPTOUT_ACTION_ID);

	if (action_id == "1") //Register Freewheel in DAA. Ads return FreeWheel company information.
	{
		//Eg. http://AdServer_host:port/ad/daa_optout?action_id=1.
		Ads_Monitor::stats_inc(Ads_Monitor::TX_DAA_OPTOUT_ACTION1);
		const char *participant_info = repo.system_variable("coop_participant_info");
		const char *freewheel_description = repo.system_variable("freewheel_description"); // not configured in OLTP
		if (participant_info && participant_info[0])
		{
			res->content("text/xml", participant_info, strlen(participant_info));
			res->ready(true);
		}
		else if (freewheel_description && freewheel_description[0])
		{
			res->content("text/xml", freewheel_description, strlen(freewheel_description));
			res->ready(true);
		}
		else
		{
			/// TODO: participant info redirect url may change.
			Ads_String redirect;
			const char *participant_info_url = repo.system_variable("coop_participant_info_url"); // not configured in OLTP
			if (participant_info_url && participant_info_url[0])
				redirect = participant_info_url;
			else
				redirect = DAA_PARICIPANT_INFO;
			res->redirect(redirect);
		}
	}
	else if (action_id == "3") //Get the Freewheel OBA/DAA opt-out cookie status in end-user's browser
	{
		//Eg. http://AdServer_host:port/ad/daa_optout?participant_id=123456&action_id=3&rd=http%3A%2F%2Fwww.aboutads.info
		Ads_Monitor::stats_inc(Ads_Monitor::TX_DAA_OPTOUT_ACTION3);
		get_daa_opt_out_cookie_status(req, res);
	}
	else if (action_id == "4") //Set FreeWheel DAA opt-out cookie in end-user's browser
	{
		//Eg. http://AdServer_host:port/ad/daa_optout?participant_id=123456&action_id=4&token=1419304741&rd=http%3A%2F%2Fwww.aboutads.info
		Ads_Monitor::stats_inc(Ads_Monitor::TX_DAA_OPTOUT_ACTION4);
		set_daa_opt_out_cookie(req, res);
	}
	else
		Ads_Monitor::stats_inc(Ads_Monitor::TX_DAA_OPTOUT_OHTER_ACTION);

	return 0;
}

/*
 * Sanity Check is done before generating response, if sanity check fails, empty response will be returned.
 * Currently, three validations are checked:
 * 1. whether the rating of advertisement are violated.
 * 2. whether slots max ads number are violated.
 * 3. whether slots max duration are violated.
 */
bool
Ads_Selector::sanity_check(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate_Ref_List &candidates_ref)
{
	std::map<Ads_Video_Slot *, std::vector<const Ads_Advertisement_Candidate_Ref *> > slot_candidate_map;

	for (auto ref : candidates_ref)
	{
		auto candidate = ref->candidate_;
		if (candidate == nullptr || candidate->lite_ == nullptr) continue;

		// 1. check advertisement rating
		if (!this->test_advertisement_rating(ctx, candidate->ad_)) return false;

		if (ref->is_fallback_ || candidate->is_proposal()) continue;

		auto vslot = ref->cast_video_slot();
		if (!vslot) continue;

		slot_candidate_map[vslot].push_back(ref);
	}

	for (auto &item : slot_candidate_map)
	{
		auto vslot = item.first;
		auto refs = item.second;

		// 2. check slot max ads number
		if (refs.size() > vslot->max_num_advertisements_) return false;

		// 3. check slot max duration
		size_t slot_duration = 0;
		std::for_each(refs.begin(), refs.end(),
				[&](const Ads_Advertisement_Candidate_Ref *ref){ slot_duration += ref->variable_ad_duration_; });
		if (slot_duration > vslot->max_duration_) return false;
	}

	return true;
}

int
Ads_Selector::build_closures_for_ad_router(Ads_Selection_Context &ctx, const Ads_Asset_Base *root_asset, const Ads_Asset_Base *root_section, const Ads_Asset_Base *original_root_asset) const
{
	if (ctx.has_built_closures_)
	{
		ADS_LOG((LP_INFO, "ctx has already built closures\n"));
		return 0;
	}
	ctx.has_built_closures_ = true;

	if (root_asset == nullptr && root_section == nullptr && original_root_asset == nullptr)
	{
		ADS_LOG((LP_INFO, "ad router: failed to build closure for ad router\n"));
		return -1;
	}

	if (root_asset != nullptr )
	{
		auto *router_closure = ctx.closure(root_asset->network_id_, true);
		if (router_closure != nullptr)
		{
			ctx.try_add_asset(router_closure, root_asset->id_, ads::MEDIA_VIDEO);
			ADS_DEBUG((LP_DEBUG, "ad router: build closure for root asset network %s\n",
				ADS_ENTITY_ID_CSTR(root_asset->network_id_)));
		}
	}

	if (root_section != nullptr)
	{
		auto *router_closure = ctx.closure(root_section->network_id_, true);
		if (router_closure != nullptr)
		{
			ctx.try_add_asset(router_closure, root_section->id_, ads::MEDIA_SITE_SECTION);
			ADS_DEBUG((LP_DEBUG, "ad router: build closure for root section network %s\n",
				ADS_ENTITY_ID_CSTR(root_section->network_id_)));
		}
	}

	return 0;
}

int
Ads_Selector::build_closures(Ads_Selection_Context *ctx, const Ads_Asset_Base *root_asset, const Ads_Asset_Base *root_section,
		const Ads_Asset *airing, const Ads_Asset_Group *channel)
{
	if (ctx->has_built_closures_)
	{
		ADS_LOG((LP_INFO, "ctx has already built closures\n"));
		return 0;
	}

	ctx->has_built_closures_ = true;
	if (root_asset != nullptr)
	{
		if (construct_asset_section_closures(ctx, root_asset, ads::MEDIA_VIDEO, ctx->closures_) < 0)
		{
			ADS_LOG((LP_ERROR, "failed to construct_asset_section_closures (asset)\n"));
			return -1;
		}
	}

	if (root_section != nullptr)
	{
		if (construct_asset_section_closures(ctx, root_section, ads::MEDIA_SITE_SECTION, ctx->closures_) < 0)
		{
			ADS_LOG((LP_ERROR, "failed to construct_asset_section_closures (site section)\n"));
			return -1;
		}
	}

	if (airing != nullptr)
	{
		if (construct_asset_section_closures(ctx, airing, ads::MEDIA_VIDEO, ctx->closures_) < 0)
		{
			ADS_LOG((LP_WARNING, "failed to construct_asset_section_closures (airing)\n"));
		}
	}
	else if (channel != nullptr)
	{
		if (construct_asset_section_closures(ctx, channel, ads::MEDIA_VIDEO, ctx->closures_) < 0)
		{
			ADS_LOG((LP_WARNING, "failed to construct_asset_section_closures (airing channel)\n"));
		}
	}

	return 0;
}

//[begin]add for asset alias for
int Ads_Selector::parse_metadata(Ads_Request *req, const Ads_Asset_Base *asset)
{
	if (asset)
	{
		if (asset->alias_items_ && (asset->alias_items_->size() > 0))
		{
			const char *metadata = nullptr;
			if (req->is_comcast_vod_ws2() || req->is_viper_csai_vod())
			{
				Ads_RString movie_paid;
				for(const auto &alias_item : asset->alias_items())
				{
					metadata = alias_item.second.c_str();
					if (!ads::trim(metadata).empty())
					{
						movie_paid = alias_item.first;
						if (::strncmp(movie_paid.c_str(), "vod://", 6) == 0)
							movie_paid = movie_paid.substr(6);
						break;
					}
				}
				ALOG(LP_DEBUG, "\n parse_metadata_on_demand comcast vod ws2 / viper csai vod ws1, movie_paid = %s\n", movie_paid.c_str());

				if (!movie_paid.empty())
				{
					req->rep()->site_section_.video_player_.video_.movie_paid_ = movie_paid.c_str();
					if (req->vod_router_ != nullptr)
						req->vod_router_->set_movie_paid(req->rep()->site_section_.video_player_.video_.movie_paid_);
				}
				else
				{
					metadata = nullptr;
				}
			}
			else
			{
				if (!req->rep()->site_section_.video_player_.video_.custom_id_.empty())
				{
					metadata = asset->get_alias_item(req->rep()->site_section_.video_player_.video_.custom_id_.c_str());
				}
			}
			return req->parse_asset_metadata(metadata);
		}
	}

	return -1;
}

void Ads_Selector::parse_profile_in_advance_for_flex_integration(Ads_Selection_Context &ctx) const
{
	if (ctx.request_ == nullptr || ctx.repository_ == nullptr || ctx.request_->rep() == nullptr)
	{
		return;
	}

	const Ads_Environment_Profile *tmp_profile = nullptr;
	if (ctx.request_->find_profile(*(ctx.repository_), ctx.request_->rep()->profile_,
	                               ADS_ENTITY_ID_STR(ctx.request_->network_id()), tmp_profile) == -1)
	{
		return;
	}

	Ads_String value;
	if (tmp_profile && tmp_profile->get_parameter("enableAssociatedAssetFOD", value) == true)
	{
		ctx.need_associated_asset_fod_network_id_ = ads::str_to_i64(value.c_str());
	}
}

void Ads_Selector::parse_profile(Ads_Selection_Context &ctx)
{
	if (ctx.request_ == nullptr || ctx.repository_ == nullptr || ctx.request_->rep() == nullptr)
	{
		ADS_LOG((LP_ERROR, "ctx.request[%p] or ctx.repository_[%p] or ctx.request_->rep() is nullptr.\n", ctx.request_, ctx.repository_));
		return;
	}
	const Ads_Asset_Base *asset = nullptr;
	ctx.find_asset(ctx.request_->asset_id_, asset);
	if (asset == nullptr ||
	    this->parse_metadata(ctx.request_, asset) < 0)
	{
		if (ctx.request_->is_comcast_vod_ws1() || ctx.request_->is_comcast_vod_ws2())
			ctx.add_error(Error::GENERAL, Error::META_DATA_EMPTY);
	}
	//ctx.profile mostly parse from rep()->profile_ directly.
	//Sometimes we need to use dimension info configured at metadata to update rep()->profile_.
	if (Ads_Request::find_profile(*(ctx.repository_), ctx.request_->rep()->profile_,
			ADS_ENTITY_ID_STR(ctx.request_->network_id()), ctx.profile_) == -1)
	{
		ctx.error(ADS_ERROR_SLOT_PROFILE_NOT_FOUND, ads::Error_Info::SEVERITY_WARN,
				ADS_ERROR_SLOT_PROFILE_NOT_FOUND_S, ctx.request_->rep()->profile_, "");
		ctx.add_error(Error::GENERAL, Error::PROFILE_NOT_FOUND);
	}

	this->parse_context_profile(&ctx);

	// flags
	if (ctx.max_initial_time_position_ <= 0) ctx.flags_ |= Ads_Selection_Context::FLAG_KEEP_INITIAL_AD;

	bool log_video_view = false;
	if (ctx.profile_)
		log_video_view = (ctx.profile_->flags_ & Ads_Environment_Profile::FLAG_FORCE_IMPLICIT_VIDEO_TRACKING);

	if (log_video_view && ctx.request_->rep()->capabilities_.log_video_view_ < 0)
	{
		ctx.request_->rep()->capabilities_.log_video_view_ = 1;
		ctx.request_->rep()->capabilities_.requires_video_callback_ = false;
	}

#if !defined(ADS_ENABLE_FORECAST)
	if (ctx.profile_)
	{
		Ads_String val;
		if(ctx.profile_->get_parameter("enableSSUSExclusivityWindowEnforcement", val))
			ctx.request_->enabled_ssus_exclusivity_ = (val == "1" && ctx.request_->rep()->live_);
		if (ctx.request_->user())
			ctx.request_->user()->request_enabled_ssus_exclusivity(ctx.request_->rep() && ctx.request_->enabled_ssus_exclusivity_);

		if (!ctx.use_actual_duration_for_improve_)
		{
			auto &profile2sample_ratio = ctx.repository_->system_config().profile2sample_ratio_config_;
			if (profile2sample_ratio.count(ads::entity::id(ctx.profile_->id_)) > 0 ||
			    profile2sample_ratio.count(-1) > 0)
			{
				ctx.use_actual_duration_for_improve_ = true;
			}
		}
	}
#endif
}

//[end]add for asset alias for

int
Ads_Selector::select(Ads_Selection_Context *ctx)
{
	int ret;

	auto local_bind = ctx->ptr(); // to make sure ctx will not be deleted inside 'select_i(ctx)'

	do {
		ret = select_i(ctx);
	} while (ctx->is_next_round_selection_required());
	return ret;
}

int
Ads_Selector::prepare_next_round_selection(Ads_Selection_Context *ctx)
{
	if (ctx->all_flattened_candidates_.empty())
		ctx->all_flattened_candidates_.swap(ctx->flattened_candidates_);
	else
	{
		ctx->all_flattened_candidates_.insert(ctx->flattened_candidates_.begin(), ctx->flattened_candidates_.end());
		ctx->flattened_candidates_.clear();
	}

	ctx->pre_final_candidates_.clear();

	if (ctx->all_final_candidates_.empty())
		ctx->all_final_candidates_.swap(ctx->final_candidates_);
	else
	{
		ctx->all_final_candidates_.insert(ctx->all_final_candidates_.end(), ctx->final_candidates_.begin(), ctx->final_candidates_.end());
		ctx->final_candidates_.clear();
	}

	if (ctx->auction_mgr_)
	{
		if (ctx->auction_mgr_->all_auctioneers_.empty())
			ctx->auction_mgr_->all_auctioneers_.swap(ctx->auction_mgr_->auctioneers_);
		else
		{
			for (auto& auctioneer : ctx->auction_mgr_->auctioneers_)
			{
				ctx->auction_mgr_->all_auctioneers_.emplace_back(std::move(auctioneer));
			}
			ctx->auction_mgr_->auctioneers_.clear();
		}

		if (ctx->auction_mgr_->std_auctioneer_)
		{
			ctx->auction_mgr_->all_std_auctioneers_.emplace_back(ctx->auction_mgr_->std_auctioneer_);
			ctx->auction_mgr_->std_auctioneer_ = nullptr;
		}

		if (ctx->auction_mgr_->sfx_auctioneer_)
		{
			ctx->auction_mgr_->all_sfx_auctioneers_.emplace_back(ctx->auction_mgr_->sfx_auctioneer_);
			ctx->auction_mgr_->sfx_auctioneer_ = nullptr;
		}

		if (ctx->auction_mgr_->all_reseller_tag_auctions_.empty())
			ctx->auction_mgr_->all_reseller_tag_auctions_.swap(ctx->auction_mgr_->reseller_tag_auctions_);
		else
		{
			ctx->auction_mgr_->all_reseller_tag_auctions_.splice(ctx->auction_mgr_->all_reseller_tag_auctions_.end(),
			                                                     ctx->auction_mgr_->reseller_tag_auctions_);
		}

		if (ctx->auction_mgr_->all_reseller_tag_loaders_.empty())
			ctx->auction_mgr_->all_reseller_tag_loaders_.swap(ctx->auction_mgr_->reseller_tag_loaders_);
		else
		{
			for (auto& loader : ctx->auction_mgr_->reseller_tag_loaders_)
			{
				ctx->auction_mgr_->all_reseller_tag_loaders_.emplace_back(std::move(loader));
			}

			ctx->auction_mgr_->reseller_tag_loaders_.clear();
		}
	}

	// For flex external bridge, keep external scheduled ad will only be filled in the initial round
#if !defined(ADS_ENABLE_FORECAST)
	if (ctx->scheduler_->has_schedule())
	{
		ctx->scheduler_->schedule_ad_info_.clear();
	}
#endif
	if (ctx->has_external_bridge_ad())
	{
		// if there are any exteranl bridge candidates left here, those candidates are all invalid, so we need to destroy them here
		// all those vaild candidates all have been moved into ctx->final_candidates_ already
		ctx->external_router_->destroy_external_bridge_ad();
	}

	if (ctx->mkpl_executor_->post_filling_slots(ctx) == 0)
	{
		++ctx->selection_rounds_;
		return 1;
	}

	return 0;
}

int
Ads_Selector::select_i(Ads_Selection_Context *ctx)
{
	std::string method_name;
	GET_METHOD_NAME(method_name);
	int ret = 0;

	if (ctx->is_next_round_selection_required())
	{
		ctx->flags_ &= ~Ads_Selection_Context::FLAG_NEXT_ROUND_SELECTION_REQUIRED;
		if (ctx->mkpl_executor_->is_initialized())
		{
			goto __MKPL_EXECUTING_ENTRY;
		}
		else
		{
			ADS_ASSERT(!"unexpected status for requiring next round selection!");
		}
	}

	if (ctx->request_ && ctx->request_->format_== Ads_Request::SSVT)
	{
		if (!ctx->is_restarted())
		{
			ctx->flags_ |= Ads_Selection_Context::FLAG_NEEDS_EXTERNAL_CREATIVES;
			try_reschedule_selection(ctx);
			return 1;
		}
		else
		{
			log_external_transaction_for_sstf_api(ctx);
			return 0;
		}
	}

	if (ctx->is_restarted())
	{
		if (ctx->stage_ == 1) { goto __STAGE_2; }
		else if (ctx->stage_ == 6) { goto __STAGE_7; }
		else if (ctx->stage_ == 8) { goto __STAGE_9; }
		else if (ctx->stage_ == 9) { goto __STAGE_10; }
		else if (ctx->stage_ == 10) { goto __STAGE_11; }
	}

////////////////////////////////////////////////////////////////////////////////
//
// Phase 0. Initializing selection context
//
////////////////////////////////////////////////////////////////////////////////

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG0((LP_TRACE,
		            "Ads_Selector::select: ------------------------------------------------------------\n"
		            "Ads_Selector::select:                PHASE 0: Initializing selection context      \n"
		            "Ads_Selector::select: ------------------------------------------------------------\n"
		           ));
	}

	if (ctx->listeners_)
		this->notify_selection_listeners(ctx, Ads_Selection_Listener::STAGE_START);

	if (ctx->request_->rep()->capabilities_.transparent_proxy_mode_)
	{
		ctx->flags_ |= Ads_Selection_Context::FLAG_COOKIE_DOMAIN_LEVEL_UP;
	}
	ret = initialize_selection_context(ctx);

	if (ctx->listeners_)
		this->notify_selection_listeners(ctx, Ads_Selection_Listener::STAGE_0);

	/// reschedule for deep lookup
	if (!ctx->active_)
	{
		ADS_DEBUG((LP_TRACE, "rescheduling ad selection\n"));
#if defined(ADS_ENABLE_LIBADS)
		ctx->in_slow_selection_ = true;
#endif
		return ret;
	}
	if (ctx->request_->rep()->capabilities_.skips_ad_selection_)
	{
		ADS_DEBUG((LP_TRACE, "skipping ad selection\n"));
	  	if (ctx->root_asset_)
		{
			ctx->networks_ = ADS_ENTITY_ID_STR(ctx->distributor_id_) + ";" + ADS_ENTITY_ID_STR(ctx->root_asset_->network_id_);
			ctx->quoted_networks_ = ads::replace(";", "%3B", ctx->networks_);
		}

		ctx->response_->initialize(ctx->request_);
		this->generate_response_body(ctx);
		ctx->response_->try_build_response_node(ctx->need_external_merging(),
		                                        ctx->enable_tracking_url_router_,
		                                        ctx->opt_out_xff_in_tracking_url_router_,
		                                        ctx->request_->proxy_callback_host(),
		                                        ctx->transaction_id_);

		if (Ads_Server_Config::instance()->logger_enable_binary_log_) {
			this->log_request(ctx);
		}

		++this->stats_.n_selection_skipped_;
		Ads_Monitor::stats_inc(Ads_Monitor::SKIPPED_SELECTION);

		ADS_DEBUG((LP_DEBUG, "Exit in Phase 0 (2)\n"));
		return 0;
	}
	else
	{
		ctx->user_->update_slot_sequences(ctx->slot_controller_.get_slot_sequences());
	}

	if (ret < 0)
	{
		ADS_DEBUG((LP_ERROR, "failed to initialize selection context\n"));
		goto error_exit;
	}

	if (!ctx->asset_ && !ctx->section_)
	{
		ADS_LOG((LP_ERROR,
		           "failed to initialize selection context, no asset / site section found\n")
		         );
		goto error_exit;
	}

////////////////////////////////////////////////////////////////////////////////
//
// Phase 1. Asset & Section closure building
//
////////////////////////////////////////////////////////////////////////////////
	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG0((LP_TRACE,
		            "Ads_Selector::select: ------------------------------------------------------------\n"
		            "Ads_Selector::select:                PHASE 1: Closures building                   \n"
		            "Ads_Selector::select: ------------------------------------------------------------\n"
		           ));
	}

	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_1_0);
	ctx->stage_ = 1;

	if (ctx->is_root_asset_reset_to_ad_router_rovn())
	{
		if (build_closures_for_ad_router(*ctx, ctx->root_asset_, ctx->root_section_, ctx->original_root_asset_) < 0)
			goto error_exit;
	}
	else if (build_closures(ctx, ctx->root_asset_, ctx->root_section_, ctx->scheduler_->airing_, ctx->scheduler_->channel_) < 0)
	{
		goto error_exit;
	}
	// PHASE 1 closure num
	ctx->decision_info_.set_values(Decision_Info::INDEX_21_22, ctx->closures_.size());

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG((LP_TRACE,
		           "root asset: %s root section: %s\n", ctx->root_asset_? ADS_ASSET_ID_TAG_CSTR(ctx->root_asset_->id_): "NA", ctx->root_section_? ADS_ASSET_ID_TAG_CSTR(ctx->root_section_->id_): "NA")
		         );

		ads::for_each2nd(ctx->closures_.begin(), ctx->closures_.end(), std::mem_fun(&Ads_Asset_Section_Closure::dump));
	}

#if defined(ADS_ENABLE_FORECAST)
	if (!ctx->interested_network_set_.empty())
	{
		bool found = false;
		for (Ads_Asset_Section_Closure_Map::iterator it = ctx->closures_.begin(); it!= ctx->closures_.end(); ++it)
		{
			if (ctx->interested_network_set_.find(it->second->network_id_) != ctx->interested_network_set_.end())
			{
				found = true;
				break;
			}
		}
		if (!found)
		{
			ctx->error_flags_ |= Ads_Selection_Context::ERROR_NO_INTERESTED_CLOSURES;
			return 0;
		}
	}
#endif

	for (auto it = ctx->closures_.begin(); it != ctx->closures_.end(); ++it)
	{
		Ads_Asset_Section_Closure *closure = it->second;
		this->initialize_sorted_assets(ctx, closure, closure->assets_, closure->mutable_rbp_info().sorted_assets_, ads::MEDIA_VIDEO);
		this->initialize_sorted_assets(ctx, closure, closure->sections_, closure->mutable_rbp_info().sorted_sections_, ads::MEDIA_SITE_SECTION);
		ADS_DEBUG((LP_TRACE, "closure %s with sorted assets:%s, sorted sections:%s\n", ADS_ENTITY_ID_CSTR(closure->network_id_),
								ads::entities_to_pretty_str(closure->rbp_info().sorted_assets_).c_str(),
								ads::entities_to_pretty_str(closure->rbp_info().sorted_sections_).c_str()));
	}

	for (auto it = ctx->closures_.begin(); it!= ctx->closures_.end(); ++it)
	{
		Ads_Asset_Section_Closure *closure = it->second;
		ADS_ASSERT(closure);
		closure->fix_edges();

		if (!closure->is_full_network())
			continue;
		ctx->initialize_restrictions(closure);
        // initialize rbb prediction model
        this->get_rbb_prediction_model(ctx, closure);

		if (Ads_Server_Config::instance()->enable_debug_)
		{
			for (auto &prediction_model : closure->prediction_model_)
			{
				Ads_GUID demographic_key = prediction_model.first;
				double demographic_band_percentage = prediction_model.second;
				Ads_Repository::DEMOGRAPHIC_PLATFORM demographic_platform = (Ads_Repository::DEMOGRAPHIC_PLATFORM)ads::entity::subtype(demographic_key);
				Ads_String demographic_platform_str = Ads_Selector::demographic_platform_to_string(demographic_platform);
				Ads_String data_source_str = (ads::entity::type(demographic_key) == ADS_ENTITY_TYPE_NIELSEN_DEMOGRAPHIC) ? "NIELSEN" : "COMSCORE";
				ADS_DEBUG((LP_DEBUG, "closure %d, demographic type: %s, demographic bucket %d with percentage(%s) %0.4f\n",
							ads::entity::id(closure->network_id_),
							data_source_str.c_str(),
							ads::entity::id(demographic_key),
							demographic_platform_str.c_str(),
							demographic_band_percentage));
			}
		}
	}

	if (initialize_context_rbp(*ctx, *ctx->repository_,  *ctx->request_, *ctx->data_privacy_, *ctx->rbp_helper_) < 0)
	{
		ADS_DEBUG((LP_DEBUG, "rbp info: failed to initialize context rbp\n"));
	}

	//merge non resellable assets
	for (std::map<Ads_GUID, Ads_GUID_Pair>::const_iterator it = ctx->inventory_info_.begin(); it != ctx->inventory_info_.end(); ++it)
	{
		Ads_GUID network_id = it->first, asset_id = it->second.first, site_section_id = it->second.second;
		Ads_Asset_Section_Closure *closure = ctx->closure(network_id);
		if (!closure) continue;

		ADS_DEBUG((LP_DEBUG, "adding: network: %s, asset: %s, site section: %s\n", ADS_ENTITY_ID_CSTR(network_id), ADS_ENTITY_ID_CSTR(asset_id), ADS_ENTITY_ID_CSTR(site_section_id)));

		if (ads::entity::is_valid_id(asset_id)) ctx->try_add_asset(closure, asset_id, ads::MEDIA_VIDEO);
		if (ads::entity::is_valid_id(site_section_id)) ctx->try_add_asset(closure, site_section_id, ads::MEDIA_SITE_SECTION);
	}

	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_1_0);

__STAGE_2:
	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_1_1);
	if (!ctx->data_privacy_->is_audience_targeting_disabled())
	{
		if (ctx->audience_->enable_xdevice_extension())
			ctx->audience_->prepare_xdevice_kv_terms();

		for (auto &closure_pair : ctx->closures_)
		{
			auto *closure = closure_pair.second;
			if (!closure->is_full_network())
				continue;
			// audience kv terms
			const Ads_Data_Right &data_right = ctx->data_right_management_->data_right_of_full_network(closure->restored_network_id());
			if (closure->network_id() != ctx->audience_->root_network_id()) // CRO audience info is initialized at PHASE0, skip it
			{
				ctx->audience_->initialize_closure_audience_info(*closure, data_right, closure->mutable_audience_info());
				ctx->audience_->collect_kv_terms(*ctx->repository_, ctx->global_terms_, closure->mutable_audience_info());
				ctx->audience_->transmit_kv_terms(closure->audience_info(), closure->terms_);
			}
			if (ctx->audience_->check_xdevice_kv_collection_enablement(*ctx->repository_, closure->audience_info()))
				ctx->audience_->collect_xdevice_kv_terms(*ctx->repository_, closure->mutable_audience_info());
			// Audience Standard Attributes
			if (closure->is_marketplace() && ctx->is_cro(closure->network_id_))
			{
				std::map<Ads_GUID, Ads_GUID_Set> audience_standard_attribute_ids;
				ctx->audience_->parse_audience_standard_attributes(*ctx->repository_,
						closure->audience_info(),
						audience_standard_attribute_ids);
				for (const auto &pair : audience_standard_attribute_ids)
				{
					if (pair.first != ctx->audience_->sis_info_.config_->data_provider_id_)
					{
						for (auto audience_standard_attribute_id : pair.second)
						{
							ctx->global_terms_.insert(ads::term::make_id(Ads_Term::STANDARD_AUDIENCE, audience_standard_attribute_id));
						}
					}
				}
			}
		}
	}

	// prepare terms for each network
	for (const auto &closure_pair : ctx->closures_)
	{
		Ads_Asset_Section_Closure *closure = closure_pair.second;
		if (!closure->is_full_network())
			continue;
		dispatch_global_terms(*ctx, closure->network_id(), Ads_Term::ALL_TYPE, closure->terms_);
		{
			if (ctx->is_fourfronts_hub(closure->network_id()))
			{
				//put all terms into hub or hub supplier
				closure->terms_.insert(ctx->audience_->hub_user_terms_.begin(), ctx->audience_->hub_user_terms_.end());
			}

			if (!ctx->data_privacy_->is_audience_targeting_disabled())
			{
				if (closure->network_id() != ctx->audience_->root_network_id()) // CRO network item is collected on PHASE 0
				{
					ctx->audience_->collect_audience_items(*ctx->repository_, closure->mutable_audience_info());
					ctx->audience_->transmit_audience_terms(closure->audience_info(), closure->terms_);
				}
				if (ctx->audience_->check_xdevice_candidate_collection_enablement(*ctx->repository_, closure->audience_info()))
					ctx->audience_->collect_xdevice_audience_items(*ctx->repository_, closure->mutable_audience_info());
			}
		}
		
		{
			Ads_GUID_Set candidates;
			this->collect_targetable_candidates(ctx, Ads_Term_Criteria::CONTENT_PACKAGE, *closure, candidates);
			for (Ads_GUID_Set::const_iterator it = candidates.begin(); it != candidates.end(); ++it)
			{
				Ads_GUID id = *it;
				const Ads_Content_Package *cp = 0;
				if (ctx->repository_->find_content_package(id, cp) >= 0 && cp)
				{
					closure->content_packages_.push_back(cp);
					closure->terms_.insert(cp->term());
				}
			}
		}

		//crrently only collect inventory_split_source_ and inv owner for video cro
		if (closure->network_id_ == ctx->root_network_id(ads::MEDIA_VIDEO))
		{
			closure->inventory_split_source_id_ = ctx->inventory_split_source_id_;
			for (const Ads_Slot_Base *slot : ctx->request_slots_)
			{
				if (!ads::entity::is_valid_id(slot->carriage_inventory_owner_))
					continue;

				auto it = ctx->repository_->standard_attributes_.inv_owner_2_ca_inv_owner_map_.find(slot->carriage_inventory_owner_);
				if (it == ctx->repository_->standard_attributes_.inv_owner_2_ca_inv_owner_map_.end())
					continue;
				closure->terms_.insert(ads::term::make_id(Ads_Term::INVENTORY_OWNER, it->second));
			}
		}

		collect_listings(ctx, *closure);

		//split inventory based on carriage listing
		if (closure->network_id_ == ctx->root_network_id(ads::MEDIA_VIDEO))
		{
			if (closure->has_carriage_listing()
				&& !ctx->repository_->system_config().is_slot_splitter_disabled(closure->network_id_)
			    && !ads::entity::is_valid_id(ctx->slot_template_id_)
			    && ctx->profile_ != nullptr)
			{
				ctx->slot_controller_.try_split_video_slots_into_shared_units(ctx->profile_,
				                                                              *ctx,
				                                                              ctx->video_slots_,
				                                                              ctx->request_slots_,
				                                                              ctx->parent_slots_);
			}
			else
			{
				ADS_DEBUG((LP_DEBUG, "slot splitter: skip slot split, carriage listing network id %s, CBP id %s, has carriage listing %d\n",
					ADS_ENTITY_ID_CSTR(closure->network_id_),
					ADS_ENTITY_ID_CSTR(ctx->slot_template_id_),
					closure->has_carriage_listing()));
			}
		}

		if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
		{
			Ads_GUID_Set& terms = closure->terms_;
			ADS_DEBUG0((LP_TRACE,
						"Ads_Selector::select: extract terms from closure %s (total %d): ", ADS_LOOPED_ID_CSTR(closure->network_id_), terms.size()));
			for (Ads_GUID_Set::iterator it = terms.begin(); it != terms.end(); ++it)
				ADS_DEBUG0((LP_TRACE, "%s, ", ads::term::str(*it).c_str()));
			ADS_DEBUG0((LP_TRACE, "\n"));
		}
	}

	assign_cro_closure_slots(ctx);

	// external router handler
	if (ctx->is_mkpl_bridge_url_enabled())
	{
		ctx->external_router_ = std::make_unique<selection::External_Router>();
		auto *cro_closure = ctx->root_closure(ads::MEDIA_VIDEO);
		if (cro_closure != nullptr)
		{
			ctx->external_router_->load_external_served_slots(ctx, *cro_closure);
		}
	}

	// Initialize AB test bucket
	if (ctx->repository_)
	{
		if (initialize_ab_test_buckets(ctx) < 0)
		{
			ADS_LOG((LP_ERROR, "Erro during AB test's collection/bucket initialization\n"));
		}
	}

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
#if !defined(ADS_ENABLE_FORECAST)
		if (ctx->repository_ != nullptr)
			ctx->audience_->debug_output_all_closure_audience_item(*ctx->repository_, ctx->closures_);
#endif
		ADS_DEBUG((LP_TRACE,
		           "root asset: %s root section: %s\n", ctx->root_asset_? ADS_ASSET_ID_CSTR(ctx->root_asset_->id_): "NA", ctx->root_section_? ADS_ASSET_ID_CSTR(ctx->root_section_->id_): "NA")
		         );

		ADS_DEBUG((LP_TRACE, "after merge, closures (total %d)\n", ctx->closures_.size()));
		ads::for_each2nd(ctx->closures_.begin(), ctx->closures_.end(), std::mem_fun(&Ads_Asset_Section_Closure::dump));
	}

	if (ctx->listeners_)
		this->notify_selection_listeners(ctx, Ads_Selection_Listener::STAGE_1);
	if (Ads_Server_Config::instance()->enable_performance_debug_)
	{
		ctx->insight_info_.append("p1_closure_num", ctx->closures_.size());
	}
	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_1_1);

////////////////////////////////////////////////////////////////////////////////
//
// Phase 2. MRM rules targeting
//
////////////////////////////////////////////////////////////////////////////////

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG0((LP_TRACE,
		            "Ads_Selector::select: ------------------------------------------------------------\n"
		            "Ads_Selector::select:                PHASE 2: Collecting MRM rules                \n"
		            "Ads_Selector::select: ------------------------------------------------------------\n"
		           ));
	}

	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_2_0);
	ctx->stage_ = 2;

	if (!ctx->request_->o().watched_rules_.empty())
	{
		this->load_watched_rules(ctx);
	}
	if (!ctx->request_->o().watched_deals_.empty())
	{
		this->load_watched_deals(ctx);
	}
	if (!ctx->request_->o().watched_buyer_groups_.empty())
	{
		this->load_watched_buyer_groups(ctx);
	}
	if (!ctx->request_->o().watched_orders_.empty())
	{
		this->load_watched_mkpl_orders(ctx);
	}
	if (!ctx->request_->o().pg_creative_overrides_.empty())
	{
		this->load_pg_override_creatives(ctx);
	}
	ctx->mkpl_executor_->initialize(ctx);

	for (Ads_Asset_Section_Closure_Map::iterator it = ctx->closures_.begin(); it!= ctx->closures_.end(); ++it)
	{
		Ads_Asset_Section_Closure *closure = it->second;
		ADS_ASSERT(closure);
		if (!closure || !closure->is_full_network()) continue;

		std::vector<Ads_MRM_Rule_Applicability_Checker*> mrm_rule_applicability_checkers;
		selection::Auction_Manager::MRM_Rule_Checker::Try_Append_To_Checkers _try_append_auction_rule_checker(*ctx, *closure, mrm_rule_applicability_checkers);

		// handle new rules
		Network_Slot_Rule_Map network_slot_rules;
		if (Ads_Selector::instance()->collect_targetable_mrm_rules(ctx, closure, mrm_rule_applicability_checkers, network_slot_rules) < 0)
		{
			ADS_DEBUG((LP_ERROR, "Failed to collect new mrm rule\n"));
		}
		if (Ads_Selector::instance()->grant_applicable_mrm_rules(ctx, closure, network_slot_rules) < 0)
		{
			ADS_DEBUG((LP_ERROR, "Failed to grant new mrm rule\n"));
		}

		if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
		{
			ADS_DEBUG((LP_TRACE, "working with closure %s\n", ADS_LOOPED_ID_CSTR(closure->network_id_)));
		}

		if (ctx->root_asset_ && this->grant_applicable_access_rules(ctx, closure, ads::MEDIA_VIDEO, ads::MEDIA_VIDEO) < 0)
		{
			ADS_DEBUG((LP_ERROR,
			           "failed to grant_applicable_access_rules\n")
			         );

			goto error_exit;
		}

		if (ctx->root_section_ && this->grant_applicable_access_rules(ctx, closure, ads::MEDIA_SITE_SECTION, ads::MEDIA_SITE_SECTION) < 0)
		{
			ADS_DEBUG((LP_ERROR,
			           "failed to grant_applicable_access_rules\n")
			         );

			goto error_exit;
		}

		if (ctx->root_asset_ && this->grant_applicable_access_rules(ctx, closure, ads::MEDIA_VIDEO, ads::MEDIA_SITE_SECTION) < 0)
		{
			ADS_DEBUG((LP_ERROR,
			           "failed to grant_applicable_access_rules\n")
			         );

			goto error_exit;
		}

		if (ctx->root_section_ && this->grant_applicable_access_rules(ctx, closure, ads::MEDIA_SITE_SECTION, ads::MEDIA_VIDEO) < 0)
		{
			ADS_DEBUG((LP_ERROR,
			           "failed to grant_applicable_access_rules\n")
			         );

			goto error_exit;
		}

		if (this->merge_old_and_new_mrm_rules(ctx, closure) < 0)
		{
			ADS_DEBUG((LP_ERROR,
			           "failed to merge old and new mrm rule\n")
			         );

			goto error_exit;
		}
	}

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG((LP_TRACE,
		           "asset closures (total %d)\n", ctx->closures_.size())
		         );
		ads::for_each2nd(ctx->closures_.begin(), ctx->closures_.end(), std::mem_fun(&Ads_Asset_Section_Closure::dump));
	}

	if (ctx->listeners_)
		this->notify_selection_listeners(ctx, Ads_Selection_Listener::STAGE_2);
	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_2_0);

////////////////////////////////////////////////////////////////////////////////
//
// Phase 3. MRM rules guarantee
//
////////////////////////////////////////////////////////////////////////////////

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG0((LP_TRACE,
		            "Ads_Selector::select: ------------------------------------------------------------\n"
		            "Ads_Selector::select:                PHASE 3: Checking guaranteed MRM rules       \n"
		            "Ads_Selector::select: ------------------------------------------------------------\n"
		           ));
	}

	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_3_0);
	ctx->stage_ = 3;

	if (ctx->root_asset_ && this->propagate_hard_guaranteed_rules(ctx, ads::MEDIA_VIDEO) < 0)
	{
		ADS_DEBUG((LP_ERROR, "failed to propagate_hard_guaranteed_rules (ASSET)\n"));
		goto error_exit;
	}

	if (ctx->root_section_ && this->propagate_hard_guaranteed_rules(ctx, ads::MEDIA_SITE_SECTION) < 0)
	{
		ADS_DEBUG((LP_ERROR, "failed to propagate_hard_guaranteed_rules (SITE_SECTION)\n"));
		goto error_exit;
	}

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG((LP_TRACE, "closures (total %d)\n", ctx->closures_.size()));
		ads::for_each2nd(ctx->closures_.begin(), ctx->closures_.end(), std::mem_fun(&Ads_Asset_Section_Closure::dump));
	}

	if (ctx->listeners_)
		this->notify_selection_listeners(ctx, Ads_Selection_Listener::STAGE_3);
	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_3_0);

////////////////////////////////////////////////////////////////////////////////
//
// Phase 4. MRM connectivity
//
////////////////////////////////////////////////////////////////////////////////

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG0((LP_TRACE,
		            "Ads_Selector::select: ------------------------------------------------------------\n"
		            "Ads_Selector::select:                PHASE 4: MRM connectivity probing            \n"
		            "Ads_Selector::select: ------------------------------------------------------------\n"
		           ));
	}

	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_4_0);
	ctx->stage_ = 4;

	if (ctx->root_asset_)
	{
		if (this->probe_access_rule_connectivity(ctx, ads::MEDIA_VIDEO, ctx->closures_, ctx->root_asset_, ctx->access_paths()) < 0)
		{
			ADS_DEBUG((LP_ERROR, "failed to probe_access_rule_connectivity\n"));
			goto error_exit;
		}
	}

	if (ctx->root_section_)
	{
		if (this->probe_access_rule_connectivity(ctx, ads::MEDIA_SITE_SECTION, ctx->closures_, ctx->root_section_, ctx->access_paths()) < 0)
		{
			ADS_DEBUG((LP_ERROR, "failed to probe_MRM_connectivity\n"));
			goto error_exit;
		}
	}

	///TODO: improve it with a more efficient way, cleanup unreachable edge, and fix available rules of edge
	for (Ads_Asset_Section_Closure_Map::iterator it = ctx->closures_.begin(); it!= ctx->closures_.end(); ++it)
	{
		Ads_Asset_Section_Closure *closure = it->second;
		for (auto sit = closure->slot_references_.begin(); sit != closure->slot_references_.end(); ++sit)
		{
			Ads_Slot_Base *slot = &sit->second.slot_;
			bool unreachable = (!sit->second.resellable_ && sit->second.guaranteed_ == 0);

			auto process_slot_rules = [&](Ads_Asset_Section_Closure::Edge::Slot_Rule_Map& slot_rules, bool clear_if_unreachable)
			{
				auto it = slot_rules.find(slot);
				if (it != slot_rules.end())
				{
					auto &slot_rule = it->second;
					if (unreachable)
					{
						for (const auto *rule : slot_rule.available_rules_)
						{
							if (ctx->is_watched(*rule))
								ctx->log_watched_info(*rule, slot, "RULE_NETWORK_IS_UNREACHABLE");
						}

						slot_rule.reachable_ = false;
						if (clear_if_unreachable)
							slot_rule.available_rules_.clear();
					}
					else
						closure->remove_upstream_targeting_unmatched_rules(ctx, slot, slot_rule.available_rules_);
				}
			};

			for (auto cit = closure->relations_.begin(); cit != closure->relations_.end(); ++cit)
			{
				Ads_Asset_Section_Closure::Relation *relation = cit->second;
				for (auto eit = relation->edges(ads::MEDIA_VIDEO).begin(); eit != relation->edges(ads::MEDIA_VIDEO).end(); ++eit)
				{
					Ads_Asset_Section_Closure::Edge *edge = eit->second;
					process_slot_rules(edge->slot_rules_, false);
				}

				for (auto eit = relation->edges(ads::MEDIA_SITE_SECTION).begin(); eit != relation->edges(ads::MEDIA_SITE_SECTION).end(); ++eit)
				{
					Ads_Asset_Section_Closure::Edge *edge = eit->second;
					process_slot_rules(edge->slot_rules_, false);
				}

				process_slot_rules(relation->selected_slot_rules_, true);
			}

			if (!unreachable)
				closure->update_slot_opportunity_rules(ctx, slot);
		}

#if 0
		const Ads_Network * network = 0;
		REPO_FIND_ENTITY_CONTINUE(ctx->repository_, network, closure->network_id_, network);
		if (network->config() && network->config()->restrict_data_visibility_to_reseller_)
		{
			ctx->enable_data_visibility_checking_ = true;
		}
#endif
	}
	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		for (std::map<Ads_Slot_Base *, Ads_MRM_Rule_Path_Map>::const_iterator it = ctx->access_paths().begin();
			 it != ctx->access_paths().end();
			 ++it)
		{
			ADS_DEBUG((LP_TRACE, "paths to each network on slot(%s):\n", it->first->custom_id_.c_str()));
			ads::for_each2nd(it->second.begin(), it->second.end(), std::mem_fun(&Ads_MRM_Rule_Path::dump));
		}
	}

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG((LP_TRACE, "closures (total %d)\n", ctx->closures_.size()));
		ads::for_each2nd(ctx->closures_.begin(), ctx->closures_.end(), std::mem_fun(&Ads_Asset_Section_Closure::dump));
	}

	if (ctx->listeners_)
		this->notify_selection_listeners(ctx, Ads_Selection_Listener::STAGE_4);
	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_4_0);

////////////////////////////////////////////////////////////////////////////////
//
// Phase 5. Merge closures (REMOVED)
//
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
//
// Phase 6. Advertisement targeting
//
////////////////////////////////////////////////////////////////////////////////
	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_6_0);
	ctx->stage_ = 6;
	{
		if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
		{
			ADS_DEBUG0((LP_TRACE,
			            "Ads_Selector::select: ------------------------------------------------------------\n"
			            "Ads_Selector::select:                PHASE 6: Advertisement targeting             \n"
			            "Ads_Selector::select: ------------------------------------------------------------\n"
			           ));
		}

		if (!ctx->request_->o().watched_ads_.empty())
			this->load_watched_candidate_advertisements(ctx, &ctx->watched_candidates_);

		if (!ctx->request_->o().watched_criterias_.empty())
		{
			Ads_String_List items;
			ads::split(ctx->request_->o().watched_criterias_, items, ',');

			for (Ads_String_List::const_iterator it = items.begin(); it != items.end(); ++it)
			{
				const Ads_String& s = *it;
				Ads_GUID criteria_id = ads::entity::make_id(ADS_ENTITY_TYPE_TARGETING_CRITERIA, ads::str_to_i64(s));
				if (!ads::entity::is_valid_id(criteria_id))
				{
					ADS_DEBUG((LP_ERROR, "invalid criteria: %s\n", s.c_str()));
					continue;
				}

				const Ads_Term_Criteria *c = nullptr;
				if (ctx->repository_->find_criteria(criteria_id, c) < 0 || !c)
				{
					ADS_DEBUG((LP_ERROR, "criteria %s not found\n", ADS_ENTITY_ID_CSTR(criteria_id)));
					continue;
				}

				std::list<const Ads_Term_Criteria*> pending{c};

				while (!pending.empty())
				{
					const auto *criteria = pending.front();
					pending.pop_front();

					ctx->watched_criterias_[criteria->id_] = 0;

					pending.insert(pending.end(), criteria->children_.begin(), criteria->children_.end());
					pending.insert(pending.end(), criteria->negative_children_.begin(), criteria->negative_children_.end());
				}
			}
		}

		///3. ad targeting

		ctx->update_candidate_restriction_tracking_status();

		/// prepare terms for each network
		Ads_GUID root_network_id = ctx->root_asset_? ctx->root_asset_->network_id_: ctx->root_section_->network_id_;
		Ads_String networks = ADS_ENTITY_ID_STR(ctx->distributor_id_) + ";" + ADS_ENTITY_ID_STR(root_network_id);

		for (const auto &it : ctx->closures_)
		{
			Ads_Asset_Section_Closure *closure = it.second;
			//closure->terms_.insert(ctx->global_terms_.begin(), ctx->global_terms_.end());
			if (closure->num_competing_ad_score_ > 0) ctx->log_competing_ad_score_ = true;

			Ads_GUID network_id = ads::entity::restore_looped_id(closure->network_id_);
			if (closure->is_full_network() && network_id != ctx->distributor_id_ && network_id != root_network_id)
			{
				if (!networks.empty()) networks += ";";
				networks += ADS_ENTITY_ID_STR(closure->network_id_);

				//TODO: log all the reseller networks?
				// ctx->log_reseller_networks_.insert(closure->network_id_);
			}

			if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
			{
				Ads_GUID_Set& terms = closure->terms_;
				ADS_DEBUG0((LP_TRACE,
				            "Ads_Selector::select: extract terms from closure %s (total %d): ", ADS_LOOPED_ID_CSTR(closure->network_id_), terms.size()));
				for (Ads_GUID_Set::iterator it = terms.begin(); it != terms.end(); ++it)
					ADS_DEBUG0((LP_TRACE, "%s, ", ads::term::str(*it).c_str()));
				ADS_DEBUG0((LP_TRACE, "\n"));
				Ads_Audience::debug_output_closure_xdevice_policys(closure->network_id_, closure->audience_info().policy_terms_);
			}
		}

		ctx->networks_ = networks;
		ctx->quoted_networks_ = ads::replace(";", "%3B", networks);

		///load blacklist and whitelist: allow & disallowed
		if (!ctx->request_->o().allowed_ads_.empty()) this->load_watched_candidate_advertisements(ctx, 0);

		///load explicit candidate list
		if (ctx->request_->has_explicit_candidates())
		{
			this->load_explicit_candidate_advertisements(ctx);
		}
		else
		{
		/// only when no explicit candidates
		for (const auto &it : ctx->closures_)
		{
			Ads_Asset_Section_Closure *closure = it.second;
			ADS_ASSERT(closure);

			/// dead network
			if (!closure || !closure->is_active() || !closure->is_full_network()) continue;

			if (Ads_Server_Config::instance()->enable_performance_debug_)
			{
				ctx->insight_info_.append("p6_term_num", (closure->terms_.size() + closure->audience_info().policy_terms_.size()));
			}
			if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
			{
				ADS_DEBUG0((LP_TRACE,
				            "Ads_Selector::select: ............................................................\n"
				            "Ads_Selector::select: working with closure %s\n", ADS_LOOPED_ID_CSTR(closure->network_id_)));
			}

			ctx->decision_info_.add_values(Decision_Info::INDEX_6); // closure num

			Ads_GUID_Set candidates, banned_candidates;
			//Part 1: collect network level ad candidates
			this->collect_targetable_advertisements(ctx, *closure, closure->terms_, candidates, &banned_candidates);
			ctx->decision_info_.add_values(Decision_Info::INDEX_8, banned_candidates.size());

			//Part 2: collect policy level ad candidates if current closure enables plc level cross device functionality.
			if (!ctx->data_privacy_->is_audience_targeting_disabled()
				&& ctx->audience_->check_xdevice_candidate_collection_enablement(*ctx->repository_, closure->audience_info())
				&& !ctx->request_->is_live_linear())
			{
				ctx->audience_->collect_xdevice_advertisements(*ctx->repository_, *closure, candidates);
			}

			ctx->decision_info_.add_values(Decision_Info::INDEX_7, candidates.size());

			if (closure->is_looped_seller() && !candidates.empty())
				ctx->has_looped_ad_candidates_ = true;

			ctx->initial_candidates_.insert(candidates.begin(), candidates.end());
#if defined(ADS_ENABLE_FORECAST)
			if (ctx->enable_collect_targeting_) continue;
#endif

			Ads_Advertisement_Candidate_Lite_Set flattened_candidates;
			this->flatten_candidate_advertisements(ctx, candidates, *closure, flattened_candidates);

			if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
			{
				ADS_DEBUG((LP_TRACE, "%d more flattened candidates collected for network %s: %s\n", flattened_candidates.size()
					, ADS_LOOPED_ID_CSTR(closure->network_id_)
					, Ads_Selection_Context::candidate_set_str(flattened_candidates, true).c_str()));
			}

			ctx->flattened_candidates_.insert(flattened_candidates.begin(), flattened_candidates.end());
		}
		}
#if defined(ADS_ENABLE_FORECAST)
		for (Ads_GUID ad_id : ctx->preset_flattened_candidates_)
		{
			Ads_Advertisement_Candidate_Lite *ad_lite = ctx->find_advertisement_candidate_lite(ad_id, nullptr, &selection::Dummy_Ad_Owner::instance());
			if (ad_lite != nullptr)
				ctx->flattened_candidates_.insert(ad_lite);
		}
#endif
	}
#if defined(ADS_ENABLE_FORECAST)
	if (ctx->enable_collect_targeting_)
	{
		return 0;
	}
#endif

	ctx->decision_info_.set_values(Decision_Info::INDEX_9, ctx->flattened_candidates_.size());

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG((LP_TRACE,
		           "candidate ad's  (total %d): %s\n", ctx->flattened_candidates_.size(), CANDIDATE_LPIDS_CSTR(ctx->flattened_candidates_)));
	}

	///save flattened candidate list
#if !defined(ADS_ENABLE_FORECAST)
	if (ctx->request_flags() & Ads_Request::Smart_Rep::DUMP_SELECTION)
#endif
	{
		for (const Ads_Advertisement_Candidate_Lite *ad_lite : ctx->flattened_candidates_)
		{
			ctx->flattened_candidates_saved_.push_back(ads::entity::make_looped_id(ad_lite->id(), ad_lite->is_looped()));
#if defined(ADS_ENABLE_FORECAST)
			ctx->preset_flattened_candidates_.insert(ads::entity::make_looped_id(ad_lite->id(), ad_lite->is_looped()));
#endif
		}
	}

	if (ctx->listeners_)
		this->notify_selection_listeners(ctx, Ads_Selection_Listener::STAGE_6);

	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_6_0);
#if defined(ADS_ENABLE_FORECAST)
	//end of forecast simulation pre selection, phase 1~6
	if (Ads_Server_Config::instance()->selector_enable_time_logging_for_performance_test_)
	{
		ADS_LOG((LP_INFO, "ADS selection[network:%s profile:%s xid:%s] time span detail: %s\n", ADS_ENTITY_ID_CSTR(ctx->network_id_),
				ctx->profile_ == nullptr ? "" : ctx->profile_->name_.c_str(), ctx->transaction_id_.c_str(), ctx->apm_.to_string().c_str()));
	}
	return 0;
#endif

__STAGE_7:
#if defined(ADS_ENABLE_FORECAST)
	for (Ads_GUID ad_id : ctx->preset_flattened_candidates_)
	{
		auto* ad_lite = ctx->find_advertisement_candidate_lite(ad_id, nullptr, &selection::Dummy_Ad_Owner::instance());
		if (ad_lite != nullptr)
			ctx->flattened_candidates_.insert(ad_lite);
	}
#endif
	ctx->mkpl_executor_->initiate(ctx);
__MKPL_EXECUTING_ENTRY:
	{
		ctx->apm_.start(Ads_Selection_Context::APM::CPU_MKPL_RETRIEVE_DEMANDS);
		int result = ctx->mkpl_executor_->retrieve_demands(ctx);
		ctx->apm_.end(Ads_Selection_Context::APM::CPU_MKPL_RETRIEVE_DEMANDS);

		if (result > 0)
		{
//			goto __MKPL_PREPARE_FOR_FILLING_SLOTS;
		}
	}

////////////////////////////////////////////////////////////////////////////////
//
// Phase 7. Advertisement tree flattening
//
////////////////////////////////////////////////////////////////////////////////

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG0((LP_TRACE,
		            "Ads_Selector::select: ------------------------------------------------------------\n"
		            "Ads_Selector::select:                PHASE 7: Advertisement tree flattening       \n"
		            "Ads_Selector::select: ------------------------------------------------------------\n"
		           ));
	}

	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_7_0);
	ctx->stage_ = 7;

	{
#if defined (ADS_ENABLE_FORECAST)
	bool check_schedule = !(ctx->request_->rep()->flags_ & (Ads_Request::Smart_Rep::BYPASS_BUDGET_CHECK | Ads_Request::Smart_Rep::PROPOSAL_IF_NO_BUDGET));
#else
	bool check_schedule = !ctx->request_->has_explicit_candidates();
#endif

	bool check_freq_cap = !(ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::BYPASS_RESTRICTION);
	bool check_budget = (check_schedule || ctx->request_->rep()->ads_.check_budget_);

	std::map<Ads_GUID, std::pair<Ads_Advertisement_Candidate_Lite*, Ads_Advertisement_Candidate_Lite*> > mirror_candidates;

	for (Ads_Advertisement_Candidate_Lite *ad_lite : ctx->flattened_candidates_)
	{
		bool looped = ad_lite->is_looped();

		auto &ad_owner = ad_lite->owner_;
		Ads_Asset_Section_Closure *closure = ad_owner.to_closure();

		if (ctx->is_1_to_n_request() && !ctx->is_ad_available_for_1_to_n_request(*ad_lite))
		{
			ADS_DEBUG((LP_DEBUG, "1:N - filter ad %s because ad is not available\n", ADS_ADVERTISEMENT_LOOPED_ID_CSTR(ad_lite->id(), looped)));
			continue;
		}

		// If there are scheduled ads, don't cap display ads which have frequency exempt setting here since they maybe companion of scheduled ads
		bool exempt_frequency_cap = (ctx->scheduler_->has_schedule() && ad_lite->frequency_exempt());
		bool is_watched = ctx->is_watched(*ad_lite);
		bool be_capped = false;

		// initialize frequency cap & check
		if (check_freq_cap  && !exempt_frequency_cap)
		{
			be_capped = !ctx->fc_->pass_frequency_cap(*ctx, *ad_lite->ad_);
		}

		//check frequency cap according to UEX setting.(ADS will collect a UEX setting based on priority of UEX set on asset, section, audience axis.)
		if (!be_capped && ctx->ux_conf_ && ctx->ux_conf_->frequency_cap_)
		{
			int ux_frequency_cap = ctx->ux_conf_->frequency_cap_->frequency_cap_;
			be_capped = !ctx->fc_->pass_uex_fc(*ctx->repository_, ux_frequency_cap, *ad_lite, ctx->request_->section_id());
		}
		if (be_capped)
		{
			if (is_watched) ctx->log_watched_info(*ad_lite, "FREQUENCY_CAP");
			ctx->decision_info_.add_values(Decision_Info::INDEX_0);
			ctx->decision_info_.reject_ads_.put(*ad_lite, Reject_Ads::FREQ_CAP);
			ctx->mkpl_executor_->log_ad_error(ctx, *ad_lite, selection::Error_Code::FREQUENCY_CAP_FAILED);
			continue;
		}
		// check brand fc
		be_capped = !ctx->fc_->pass_frequency_cap_brand(*ctx, *ad_lite);
		if (be_capped)
		{
			if (is_watched) ctx->log_watched_info(*ad_lite, "BRAND_FREQUENCY_CAP");
			ctx->decision_info_.add_values(Decision_Info::INDEX_0);
			ctx->decision_info_.reject_ads_.put(*ad_lite, Reject_Ads::FREQ_CAP);
			ctx->mkpl_executor_->log_ad_error(ctx, *ad_lite, selection::Error_Code::FREQUENCY_CAP_FAILED);
			continue;
		}
		
		//CPx custom event support check
		bool is_CPx = false, cpx_measurable = false;
		Ads_GUID triggering_concrete_event_id = -1;
		Ads_GUID_Set measurable_concrete_events;
		Ads_Advertisement_Candidate_Lite *placement_ad_lite = ad_lite->placement();
		if (ad_lite->is_cpx_placement() && ad_lite->billing_abstract_event() && placement_ad_lite)
		{
			if (ad_lite->billing_abstract_event()->concrete_events_.empty())
			{
				ADS_DEBUG((LP_TRACE, "ad %s cannot be delivered due to abstract event(%s)'s concrete events is empty.\n", ADS_ADVERTISEMENT_ID_CSTR(ad_lite->id()), ADS_ENTITY_ID_CSTR(ad_lite->billing_abstract_event()->id_)));
				if (is_watched) ctx->log_watched_info(*ad_lite, "NO_MATCHING_CONCRETE_EVENT");
				ctx->decision_info_.add_values(Decision_Info::INDEX_25);
				continue;
			}
			for (const auto& concrete_event_record_pair : ad_lite->billing_abstract_event()->concrete_events_)
			{
				if(ctx->supported_concrete_events_.find(concrete_event_record_pair.first) != ctx->supported_concrete_events_.end())
				{
					measurable_concrete_events.insert(concrete_event_record_pair.first);
					if(!cpx_measurable)
					{
						triggering_concrete_event_id = concrete_event_record_pair.first;
						cpx_measurable = true;
					}
				}
			}
			//Plc is compositional
			if (ad_lite->billing_abstract_event()->compositional_)
			{
#if defined (ADS_ENABLE_FORECAST)
				int64_t measured_gross_imps_participated = 0;
				time_t timestamp = 0;
				ctx->counter_service_->read_counter(ad_lite->placement_id(), Counter_Spec::EXT_ID_COMPOSITIONAL_MEASURED, Counter_Spec::COUNTER_ADVERTISEMENT_IMPRESSION, measured_gross_imps_participated, timestamp);
				measured_gross_imps_participated = std::max(measured_gross_imps_participated, placement_ad_lite->participating_denominator_events());
				if (measured_gross_imps_participated < ctx->repository_->system_config().cpx_min_denominator_imps_ && !cpx_measurable)
#else
				if (!cpx_measurable && (placement_ad_lite->ad()->targeted_ratio() <= 0.0 || placement_ad_lite->participating_denominator_events() < ctx->repository_->system_config().cpx_min_denominator_imps_))
#endif
				{
#if defined (ADS_ENABLE_FORECAST)
					ADS_DEBUG((LP_TRACE, "ad %s cannot be delivered into unmeasurable environment due to data in counter(ext_id=6) %d < %d (participating denominator event count is too little)\n", ADS_ADVERTISEMENT_ID_CSTR(ad_lite->id()), measured_gross_imps_participated, ctx->repository_->system_config().cpx_min_denominator_imps_));
#else
					ADS_DEBUG((LP_TRACE, "ad %s cannot be delivered into unmeasurable environment due to on-targeted ratio is 0 or participating denominator event count %d does not reach threshold %d\n", ADS_ADVERTISEMENT_ID_CSTR(ad_lite->id()), placement_ad_lite->participating_denominator_events(), ctx->repository_->system_config().cpx_min_denominator_imps_));
#endif
					if (is_watched) ctx->log_watched_info(*ad_lite, "NO_MATCHING_CONCRETE_EVENT");
					ctx->decision_info_.add_values(Decision_Info::INDEX_25);
					continue;
				}

				auto concrete_event_iter = ctx->repository_->concrete_events_.find(triggering_concrete_event_id);
				if (concrete_event_iter != ctx->repository_->concrete_events_.end())
				{
					ad_lite->provider_measured_event_id_ = concrete_event_iter->second->provider_measured_event_id_;
					//find abstract denominator id via triggering event's corresponding provider id.
					auto abstract_denominator_iter = ad_lite->billing_abstract_event()->provider_abstract_denominator_.find(concrete_event_iter->second->provider_id_);
					if (abstract_denominator_iter != ad_lite->billing_abstract_event()->provider_abstract_denominator_.end())
					{
						//find final concrete denominator event via pair<triggering event, abstract denominator id>
						auto concrete_denominator_iter = ctx->repository_->concrete_event_denominator_assignment_.find({triggering_concrete_event_id, abstract_denominator_iter->second});
						if (concrete_denominator_iter != ctx->repository_->concrete_event_denominator_assignment_.end())
							ad_lite->participating_denominator_event_id_ = concrete_denominator_iter->second;
					}
				}
			}
			//Plc is direct and Plc abstract event is not measurable.
			else if (!cpx_measurable)
			{
				ADS_DEBUG((LP_TRACE, "ad %s cannot be delivered due to billing abstract event not measurable by environment\n", ADS_ADVERTISEMENT_ID_CSTR(ad_lite->id())));
				if (is_watched) ctx->log_watched_info(*ad_lite, "NO_MATCHING_CONCRETE_EVENT");
				ctx->decision_info_.add_values(Decision_Info::INDEX_25);
				continue;
			}

			is_CPx = true;
		}

		/// fill rate check
		/// TODO check FDB-4400 sponsorship with budget
		selection::Pacing_Status pacing_st;
		this->budget_controller_.calculate_advertisement_fill_rate(
			*ctx, *(ad_lite->cast_to_ad()), &ad_lite->owner_, ad_owner.enable_daily_pacing_,
			true/*print_debug_log*/, false/*is_predicted*/, 0/*timestamp_to_predict*/, 0/*extra_imps*/,
			ctx->is_watched(*ad_lite) ? ctx->get_watched_id(*ad_lite) : ads::entity::invalid_id(0), nullptr/*debug_msg*/, pacing_st);

		bool enable_check = true;
		if (Ads_Server_Config::instance()->enable_debug_)
		{
			this->budget_controller_.print_pacing_debug_log(*ctx, *ad_lite->cast_to_ad(), pacing_st, check_budget, check_schedule, enable_check);
		}

		if (!this->budget_controller_.pass_pacing_check(*ctx, *ad_lite, pacing_st, check_budget, check_schedule, enable_check, is_watched))
		{
			continue;
		}

		// rating base buy
#if defined(ADS_ENABLE_FORECAST)
		//This is for getting RBP related info for forecasting when targeted_fill_rate is less than 0.
		if (closure && placement_ad_lite && placement_ad_lite->rbp_info_->is_rbp_placement())
#else
		if (placement_ad_lite && placement_ad_lite->rbp_info_->is_rbp_placement() && pacing_st.targeted_fill_rate() >= 0)
#endif
		{
			if (ad_lite->rbp_info_->initialize(*ctx->repository_, *ctx->request_, *ctx->rbp_helper_, ad_owner, pacing_st.targeted_fill_rate()) < 0)
				continue;

#if defined(ADS_ENABLE_FORECAST)
			ctx->placements_[placement_ad_lite->ad()->placement_id()].trackable_by_data_source_ = ad_lite->rbp_info_->is_measurable();
			if(placement_ad_lite->data_source() == Ads_Advertisement::DATA_SOURCE_NIELSEN)
			{
				ctx->placements_[placement_ad_lite->ad()->placement_id()].on_target_ratio_ = ad_lite->rbp_info_->calculate_on_target_probability(*closure, Ads_Repository::DEMOGRAPHIC_PLATFORM_TOTAL);
				ctx->placements_[placement_ad_lite->ad()->placement_id()].effective_platform_ = Ads_Repository::DEMOGRAPHIC_PLATFORM_TOTAL;
			}
			else
			{
				ctx->placements_[placement_ad_lite->ad()->placement_id()].on_target_ratio_ = ad_lite->rbp_info_->get_on_target_probability();
				ctx->placements_[placement_ad_lite->ad()->placement_id()].effective_platform_ = ad_lite->rbp_info_->get_on_target_demo_platform();
			}
			if (!ctx->has_explicit_candidates() && pacing_st.targeted_fill_rate() >= 0)
#else
			if (check_schedule)
#endif
			{
				auto failed_reason = Advertisement_RBP_Info::RBP_NO_DELIVERY_REASON::REASON_UNKNOWN;
				if(!ad_lite->rbp_info_->is_deliverable(*ctx->rbp_helper_, ad_owner.rbp_info(), failed_reason))
				{
					ctx->decision_info_.add_values(Decision_Info::INDEX_25);
					if(is_watched)
						ctx->log_watched_info(*ad_lite, ad_lite->rbp_info_->to_string(failed_reason));
					continue;
				}
			}
		}

		auto slot_accesses = ad_owner.all_slot_accesses();
		// no slot for the network
		if (slot_accesses.empty())
		{
			if (is_watched) ctx->log_watched_info(*ad_lite, "NO_SLOT_ASSINGED_THROUGH_MRM");
			ctx->decision_info_.reject_ads_.put(*ad_lite, Reject_Ads::NO_SLOT_ASSIGNED);
			ctx->decision_info_.add_values(Decision_Info::INDEX_2);
			continue;
		}

		if (is_watched)
		{
			for (const Ads_Slot_Base *slot : ctx->request_slots_)
			{
				if (ad_owner.slot_access(slot) == nullptr)
				{
					ctx->log_watched_info(*ad_lite, slot, "NOT_RESELLABLE");
				}
			}
		}

		bool sponsored = ctx->sponsorship_controller_->check_ad_sponsorship(*ad_lite, ctx->request_->rep()->flags_, pacing_st.met_schedule(), pacing_st.met_ext_schedule());
		/// filter slots
		Ads_Slot_List slots;
		for (const auto *slot_access : slot_accesses)
		{
			Ads_Slot_Base *slot = slot_access->slot();

			///XXX: workaround for creative preview, ommiting tracking slot
			if (ads::entity::type(ad_lite->id()) == ADS_ENTITY_TYPE_SYSTEM_ADVERTISEMENT)
			{
				if (slot != ctx->tracking_slot_) slots.push_back(slot);
				continue;
			}

			/// UEC removed slots
			if (slot->env() == ads::ENV_VIDEO || ad_lite->is_initial_display())
			{
				std::map<Ads_GUID, bool>::const_iterator it = slot->ad_unit_blacklist_.find(ad_lite->ad_unit()->id_);
				//if it is UEX removed slot, check whether current ad is sponsorship. This part is related with feature http://wiki.freewheel.tv/display/viqa/Summary+of+Sponsorship#SummaryofSponsorship-SponsorshipV.SExemptionfromUEX
				if (slot->flags_ & Ads_Slot_Base::FLAG_SPONSORSHIP_ONLY || slot->flags_ & Ads_Slot_Base::FLAG_PLACEHOLDER || it != slot->ad_unit_blacklist_.end())
				{
				//if it is not sponsorship ad, this slot can not be used for current ad even if UEX set sponsorship exemption(FLAG_SPONSORSHIP_ONLY).
				if (!ad_lite->is_sponsorship())
				{
					if (is_watched) ctx->log_watched_info(*ad_lite, slot, "SPONSORSHIP_ONLY");
					ADS_DEBUG((LP_DEBUG, "skip: SPONSORSHIP_ONLY ad %s for slot %s\n", ADS_LOOPED_ID_CSTR2(ad_lite->id(), looped), slot->custom_id_.c_str()));
					continue;
				}
				//if current sponsorship ad is "Not Exempt from User Experience Settings",
				//the slot can not be used because it is UEX removed slot and the ad must obey UEX setting.
				if (ad_lite->user_experience_exemption() == Ads_Advertisement::TRUMP_NOT_DELIVER)
				{
					if (is_watched) ctx->log_watched_info(*ad_lite, slot, "SPONSORSHIP_TRUMPED");
					ADS_DEBUG((LP_DEBUG, "skip: SPONSORSHIP_TRUMPED ad %s for slot %s\n", ADS_LOOPED_ID_CSTR2(ad_lite->id(), looped), slot->custom_id_.c_str()));
					continue;
				}
				else if (ad_lite->user_experience_exemption() == Ads_Advertisement::NOT_TRUMP)
				{
					//if current sponsorship ad set "Let Content's Setting Dictate Sponsorship Exemptions" and UEX setting does not set "sponsorship exemption", it will be FLAG_PLACEHOLDER and not be used by current ad.
					if (slot->flags_ & Ads_Slot_Base::FLAG_PLACEHOLDER || (it != slot->ad_unit_blacklist_.end() && !it->second))
					{
						if (is_watched) ctx->log_watched_info(*ad_lite, slot, "SLOT_REMOVED");
						ADS_DEBUG((LP_DEBUG, "skip: SLOT_REMOVED ad %s for slot %s\n", ADS_LOOPED_ID_CSTR2(ad_lite->id(), looped), slot->custom_id_.c_str()));
						continue;
					}
				}
				}
			}

			///XXX: workaround for tracking ad, all slot or tracking slot
			if (ad_lite->is_tracking() && ctx->is_tracking())
			{
				slots.push_back(slot);
				continue;
			}

			bool slot_sponsored = sponsored && slot_access->sponsorship_enabled();
			if (sponsored && !slot_sponsored)
			{
				ADS_DEBUG((LP_TRACE, "sponsorship_manager: sponsorship ad %s is trumped by downstream reseller on slot %s due to configuration of HG rule with trump sponsorship enabled\n",
				           ADS_ENTITY_ID_CSTR(ad_lite->id()), slot->custom_id_.c_str()));
			}
			/// resellable?
			if (ad_lite->is_external() && !ad_lite->is_mkpl_programmatic_placeholder())
			{
				const Ads_MRM_Rule *dummy = nullptr;
				Ads_GUID rule_ext_id = -1;
				if (!pick_mrm_rule_for_external_ad(ctx, slot, *ad_lite, dummy, rule_ext_id))
				{
					if (is_watched) ctx->log_watched_info(*ad_lite, slot, "NO_RESELL_RULE");
					ADS_DEBUG((LP_DEBUG, "skip: NO_RESELL_RULE(external) ad %s for slot %s\n", ADS_LOOPED_ID_CSTR2(ad_lite->id(), looped), slot->custom_id_.c_str()));
					continue;
				}
			}
			else if (!slot_sponsored)
			{
#if defined(ADS_ENABLE_FORECAST)
				// for custom portfolio, allowed into candiate even if resellable = false as long as slot_ref.guranteed_ = -1
				if ((ad_lite->is_portfolio() && closure != nullptr && !closure->is_slot_active(slot))
					|| (!ad_lite->is_portfolio() && !slot_access->sellable()))
#else
				if (!slot_access->sellable())
#endif
				{
					if (is_watched)
					{
						ctx->log_watched_info(*ad_lite, slot, "NOT_RESELLABLE");
					}
					ADS_DEBUG((LP_DEBUG, "skip: NOT_RESELLABLE(resellable) ad %s for slot %s\n", ADS_LOOPED_ID_CSTR2(ad_lite->id(), looped), slot->custom_id_.c_str()));
					continue;
				}

				if (slot_access->promo_only() && !ad_lite->is_promo())
				{
					if (is_watched)
					{
						ctx->log_watched_info(*ad_lite, slot, "PROMO_ONLY_RULE");
					}
					ADS_DEBUG((LP_DEBUG, "skip: PROMO_ONLY_RULE ad %s for slot %s\n", ADS_LOOPED_ID_CSTR2(ad_lite->id(), looped), slot->custom_id_.c_str()));
					continue;
				}
			}

			bool is_slot_applicable = true;

			//do not allow ad chooser to be delivered as Full reseller ad
			if (ad_lite->is_ad_chooser() &&
			    ad_lite->network_id() != ctx->root_network_id(ads::MEDIA_VIDEO))
			{
				is_slot_applicable = false;
			}
			// ad unit compatible?
			if (is_slot_applicable &&
			    !ctx->is_advertisement_compatible_with_slot(ad_lite, slot, is_watched))
			{
				is_slot_applicable = false;
			}

			/// FDB-2998 first/last in parent_slot && 2-N in parent_slot
			if (is_slot_applicable && !slot->is_tracking_only() &&
			    slot->env() == ads::ENV_VIDEO && !slots.empty())
			{
				const Ads_Video_Slot *vslot = reinterpret_cast<const Ads_Video_Slot *>(slot);
				const Ads_Slot_Base * previous_slot = slots.back();

				if (previous_slot->env() == ads::ENV_VIDEO)
				{
					const Ads_Video_Slot *previous_vslot = reinterpret_cast<const Ads_Video_Slot *>(previous_slot);

					if (vslot->parent_ && vslot->parent_ == previous_vslot->parent_)
					{
						// continue if not first && 2-N in parent_slot
						if (ad_lite->ad_unit()->position_in_slot_ > 0)
						{
							is_slot_applicable = false;
						}
						else if (ad_lite->ad_unit()->position_in_slot_ == -1)
						{
							// remove previous slot if it's not last in parent_slot
							slots.pop_back();
						}
					}
				}
			}

			if (is_slot_applicable)
			{
				slots.push_back(slot);
			}

			// record sponsorship
			if (slot_sponsored)
			{
				ctx->sponsorship_controller_->process(*ad_lite, *slot, is_slot_applicable);
			}
		}

		if (ad_lite->is_sponsorship()
			&& !ctx->sponsorship_controller_->is_valid_sponsor_ad(ad_lite->id()))
		{
			ctx->decision_info_.add_values(Decision_Info::INDEX_3);
			continue;
		}

		if (slots.empty() && is_watched) ctx->log_watched_info(*ad_lite, "NO_APPLICABLE_SLOT");

		if ((!sponsored || ad_lite->exclusivity_scope() == Ads_Advertisement::TARGETED_AD_UNIT)
			&& slots.empty())
		{
			ADS_DEBUG((LP_TRACE, "no slot for ad %s\n", ADS_ADVERTISEMENT_LOOPED_ID_CSTR(ad_lite->id(), looped)));
			ctx->decision_info_.add_values(Decision_Info::INDEX_2);
			ctx->decision_info_.reject_ads_.put(*ad_lite, Reject_Ads::NO_APPLICABLE_SLOT);
			continue;
		}

		/// all pod? (TODO: removed?)
		if (ad_lite->ad_unit() && ad_lite->ad_unit()->take_all_chances_
			&& !(ctx->root_asset_ && !ctx->request_->rep()->capabilities_.disable_ad_unit_in_multiple_slots_))
		{
			if (is_watched) ctx->log_watched_info(*ad_lite, "ALL_POD_DISALLOWED");
			if (!sponsored || ad_lite->exclusivity_scope() == Ads_Advertisement::TARGETED_AD_UNIT)
				continue;
		}

		//mark placements w ad choosers
		if (ad_lite->is_ad_chooser() && placement_ad_lite && ad_lite->network_id() == ctx->root_network_id(ads::MEDIA_VIDEO) && !looped)
			ctx->placements_[ad_lite->placement_id()].has_ad_chooser_ = true;

		ad_lite->fill_rate_ = pacing_st.fill_rate();
		ad_lite->daily_fill_rate_ = pacing_st.daily_fill_rate();
		if(ad_lite->ad_->placement_ != nullptr)
		{
			ad_lite->targeted_ratio_ = ad_lite->ad_->placement_->targeted_ratio();
		}
		ad_lite->time_ = pacing_st.life_stage();
		ad_lite->applicable_slots_.swap(slots);
		if (is_CPx)
		{
			ad_lite->is_CPx_ = true;
			ad_lite->is_CPx_measurable_ = cpx_measurable;
			ad_lite->triggering_concrete_event_id_ = triggering_concrete_event_id;
			ad_lite->measurable_concrete_events_.swap(measurable_concrete_events);

			if (ad_lite->billing_abstract_event()->compositional_)
			{
				ad_lite->flags_ |= Ads_Advertisement_Candidate_Lite::FLAG_CPX_COMPOSITIONAL;
			}
			if (ads::entity::is_valid_id(ad_lite->triggering_concrete_event_id_))
			{
				auto iter = ad_lite->billing_abstract_event()->find(ad_lite->triggering_concrete_event_id_);
				if (iter != ad_lite->billing_abstract_event()->concrete_events_.end() && iter->second.second)
				{
					ad_lite->flags_ |= Ads_Advertisement_Candidate_Lite::FLAG_CPX_INCLUDE_IN_BILLABLE_RATE;
				}
			}
		}

		if (is_watched) ad_lite->flags_ |= Ads_Advertisement_Candidate_Lite::FLAG_WATCHED;
		if (pacing_st.met_schedule()) ad_lite->flags_ |= Ads_Advertisement_Candidate_Lite::FLAG_MEET_SCHEDULE;
		if (pacing_st.met_schedule() && pacing_st.met_ext_schedule()) ad_lite->flags_ |= Ads_Advertisement_Candidate_Lite::FLAG_MEET_EXT_SCHEDULE;

		if (ctx->has_looped_ad_candidates_ && ad_lite->network_id() == ctx->root_network_id(ads::MEDIA_VIDEO))
		{
			Ads_GUID mirror_id = ad_lite->is_looped() ? ad_lite->id() : ads::entity::make_looped_id(ad_lite->id(), true);
			if(ad_lite->is_looped())
				mirror_candidates[ad_lite->id()].second = ad_lite;
			else
				mirror_candidates[ad_lite->id()].first = ad_lite;
			continue;
		}

		ctx->pre_final_candidates_.insert(std::make_pair(ad_lite->id(), ad_lite));
	}

	std::map<Ads_GUID, std::pair<Ads_Advertisement_Candidate_Lite*, Ads_Advertisement_Candidate_Lite*> >::const_iterator mit = mirror_candidates.begin();
	for (; mit != mirror_candidates.end(); ++mit)
	{
		Ads_GUID ad_id = mit->first;
		Ads_Advertisement_Candidate_Lite *normal_lite = mit->second.first, *looped_lite = mit->second.second;
		Ads_Advertisement_Candidate_Lite *lite = ctx->choose_overlapped_advertisement_candidate(normal_lite, looped_lite);

		if(normal_lite && normal_lite != lite && !normal_lite->ad_->is_portfolio())
		{
			ADS_DEBUG((LP_DEBUG, "ad %s cannot be delivered due to loop repetition\n", CANDIDATE_LITE_LPID_CSTR(normal_lite)));
			normal_lite = NULL;
		}
		if(looped_lite && looped_lite != lite && !looped_lite->ad_->is_portfolio())
		{
			ADS_DEBUG((LP_DEBUG, "ad %s cannot be delivered due to loop repetition\n", CANDIDATE_LITE_LPID_CSTR(looped_lite)));
			looped_lite = NULL;
		}
#if defined(ADS_ENABLE_FORECAST)
		if (normal_lite) ctx->pre_final_candidates_.insert(std::make_pair(ad_id, normal_lite));
		if (looped_lite) ctx->pre_final_candidates_.insert(std::make_pair(ads::entity::make_looped_id(ad_id, true), looped_lite));
#else
		if (lite)
			ctx->pre_final_candidates_.insert(std::make_pair(ad_id, lite));
#endif
	}

	ctx->sponsorship_controller_->check(*ctx);
	}

	if (ctx->is_initial_round_selection()
#if !defined(ADS_ENABLE_FORECAST)
		&& !ctx->request_->has_explicit_candidates()
#endif
	)
	{
		Ads_GUID_Set placements;

		for (Ads_Slot_List::const_iterator it = ctx->request_slots_.begin(); it != ctx->request_slots_.end(); ++it)
		{
			Ads_Slot_Base *slot = *it;
			if (slot->sponsor_candidates_.empty()) continue;

			ADS_DEBUG((LP_TRACE, "inventory of slot %s sponsored by %s\n", slot->custom_id_.c_str(), ads::entities_to_str(slot->sponsor_candidates_).c_str()));

			if (slot->sponsor_placements_.size() > 1
				// track_restrict ads will be concordant with each other
				&& ctx->num_normal_or_embedded_candidates(slot->sponsor_candidates_) > 1)
			{
				Ads_String s = ads::entities_to_str(slot->sponsor_placements_);
				ADS_DEBUG((LP_ERROR, "multiple sponsorship placements found: %s in slot %s, possibly in conflict.\n", s.c_str(), slot->custom_id_.c_str()));

				ctx->error(ADS_ERROR_MULTIPLE_SPONSORED_PLACEMENTS, ads::Error_Info::SEVERITY_WARN, ADS_ERROR_MULTIPLE_SPONSORED_PLACEMENTS_S, slot->custom_id_, s);
			}
		}
	}

	ctx->decision_info_.set_values(Decision_Info::INDEX_10, ctx->pre_final_candidates_.size());
	/// debug info
	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		Ads_GUID_Set tmp_candidates;

		for (const auto& id_candidate_pair : ctx->pre_final_candidates_)
		{
			Ads_Advertisement_Candidate_Lite *lite = id_candidate_pair.second;
			if (lite->flags_ & Ads_Advertisement_Candidate_Lite::FLAG_INVALID) continue;
			tmp_candidates.insert(id_candidate_pair.first);
		}

		ADS_DEBUG((LP_TRACE, "flatten candidate ads (after checking sponsorship ads) (total %d): %s\n",
					tmp_candidates.size(), CANDIDATE_LPIDS_CSTR(tmp_candidates)));
	}

	if (ctx->listeners_)
		this->notify_selection_listeners(ctx, Ads_Selection_Listener::STAGE_7);
	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_7_0);


////////////////////////////////////////////////////////////////////////////////
//
// Phase 8. Calculate net eCPM of advertisements
//
////////////////////////////////////////////////////////////////////////////////

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG0((LP_TRACE,
		            "Ads_Selector::select: ------------------------------------------------------------\n"
		            "Ads_Selector::select:                PHASE 8: Net eCPM calculation                \n"
		            "Ads_Selector::select: ------------------------------------------------------------\n"
		           ));
	}

	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_8_0);
	ctx->stage_ = 8;

	ctx->load_external_ad_url_overrides();
	// init pg td ads
	ctx->collect_pg_td_ads();

	if (ctx->collect_auctions() >= 0)
	{
		ctx->flags_ |= Ads_Selection_Context::FLAG_NEEDS_EXTERNAL_CANDIDATES;
	}
#if !defined(ADS_ENABLE_FORECAST)
	if (ctx->is_initial_round_selection())
	{
		if (ctx->request_->is_viper_csai_vod() &&
			ctx->collect_vod_bridge_ads_loader() >= 0)//need load external vod bridge ads
		{
			ctx->flags_ |= Ads_Selection_Context::FLAG_NEEDS_EXTERNAL_CANDIDATES;
			ADS_DEBUG((LP_DEBUG, "VOD Bridge is needed and jump out of ad selection\n"));
		}

		if (ctx->need_external_routing())
		{
			ctx->flags_ |= Ads_Selection_Context::FLAG_NEEDS_EXTERNAL_CANDIDATES;
			ADS_DEBUG((LP_DEBUG, "External Bridge is needed and jump out of ad selection\n"));
		}
	}
#endif
	if (ctx->mkpl_executor_->prepare_programmatic_auctions(ctx, true/*(ctx->flags_ & Ads_Selection_Context::FLAG_NEEDS_EXTERNAL_CANDIDATES) != 0*/) == 0)
	{
		ctx->flags_ |= Ads_Selection_Context::FLAG_NEEDS_EXTERNAL_CANDIDATES;
	}

#if defined(ADS_ENABLE_LIBADS)
	if (ctx->is_bidder_traffic() && ctx->bidding_strategy_manager_->query_hexbid_bidding_price(*ctx) >=0)
	{
		ctx->flags_ |= Ads_Selection_Context::FLAG_NEEDS_APPLYING_BIDDING_STRATEGY;
	}
#endif

	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_8_0);
#if defined(ADS_ENABLE_LIBADS)
	if (ctx->needs_applying_bidding_strategy())
	{
		ALOG(LP_DEBUG, "Applying bidding strategy is needed, jump out of ad selection\n");
		EXEC_IO("hexbid_svc");
	}
#endif

	// escape for external ad loading
	if (ctx->needs_external_candidates())
	{
#if !defined(ADS_ENABLE_FORECAST)
		ALOG(LP_DEBUG, "External candidate loading is needed and jump out of ad selection\n");
		try_reschedule_selection(ctx);
		return 1;
#else
		ALOG(LP_DEBUG, "Start to load external candidates\n");
		ctx->forecast_start_load_external_candidates();
#endif
	}

__STAGE_9:
	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_8_1);

	for (std::multimap<Ads_GUID, Ads_Advertisement_Candidate_Lite *>::iterator it = ctx->pre_final_candidates_.begin();
	     it != ctx->pre_final_candidates_.end();
	     ++it)
	{
		Ads_Advertisement_Candidate_Lite *ad = it->second;
		if (ad->flags_ & Ads_Advertisement_Candidate_Lite::FLAG_INVALID) continue;

		if (ad->is_guaranteed_deal_ad())
		{
			const auto &deal = *ad->get_deal();
			const auto *buyer_node = ad->owner_.to_mkpl_participant_node();
			bool met_schedule = deal.mkpl_order_ ? (buyer_node->inbound_order_ ? buyer_node->inbound_order_->fill_rate_.met_schedule_ : false)
								: ctx->mrm_rule_fill_rate(*deal.rule_).met_schedule_;
			//set flag for pg/pg ad with excess delivery
			if (met_schedule)
				ad->flags_ |= Ads_Advertisement_Candidate_Lite::FLAG_MEET_SCHEDULE;
		}

		// PG ISO restriction on inventory
		if (ad->is_pg_td_ad() && ad->need_check_iso(*ctx))
		{
			const auto &deal = *ad->get_deal();
			Ads_GUID entity_id = ads::entity::invalid_id(0);
			selection::Fill_Rate fill_rate = ad->fill_rate(*ctx);
			double osi_threshold = Ads_Budget_Controller::get_advertisement_iso_restriction_threshold(ctx, &ad->operation_owner(), fill_rate.time_);
			if (osi_threshold >= 0 && fill_rate.fill_rate_ > osi_threshold)
			{
				ad->flags_ |= Ads_Advertisement_Candidate_Lite::FLAG_INVALID;
				ad->set_error(selection::Error_Code::BLOCKED_BY_INVENTORY_SOURCE_OPTIMIZATION);
				ADS_DEBUG((LP_DEBUG, "filter pg deal %s, ad %s met inventory source restriction, fill_rate(%f) > top_threshold(%f);\n",
							ADS_ENTITY_ID_CSTR(deal.id_), CANDIDATE_LITE_ID_CSTR(ad), fill_rate.fill_rate_, osi_threshold));
				if (ad->is_watched())
					ctx->log_watched_info(deal, "MET_INVENTORY_SOURCE_RESTRICTION");
				continue;
			}
		}

		bool check_freq_cap = !(ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::BYPASS_RESTRICTION);
		if (check_freq_cap)
		{
			if (ad->get_deal() != nullptr)
			{
				if (!ctx->fc_->pass_frequency_cap(*ctx, *ad->get_deal()))
				{
					if (ad->is_watched()) ctx->log_watched_info(*ad, "FREQUENCY_CAP");
					ad->set_error(selection::Error_Code::FREQUENCY_CAP_REACHED);
					ctx->decision_info_.add_values(Decision_Info::INDEX_0);
					ctx->decision_info_.reject_ads_.put(*ad, Reject_Ads::FREQ_CAP);
					continue;
				}
			}
			//check market ad brand level fc
			if (ad->ad()->is_faked_ad())
			{
				if (!ctx->fc_->pass_frequency_cap_brand(*ctx, *ad))
				{
					if (ad->is_watched()) ctx->log_watched_info(*ad, "BRAND_FREQUENCY_CAP");
					ad->set_error(selection::Error_Code::BRAND_FREQUENCY_CAP_REACHED);
					ctx->decision_info_.add_values(Decision_Info::INDEX_0);
					ctx->decision_info_.reject_ads_.put(*ad, Reject_Ads::FREQ_CAP);
					continue;
				}
			}
		}

		if (ad->get_deal() && ad->ad_unit()->type_ == Ads_Slot_Restriction::SEQUENCED_VARIANT)
		{
			Ads_Slot_List &slots = ad->applicable_slots_;
			auto it = std::remove_if(slots.begin(), slots.end(), [ctx, ad](const Ads_Slot_Base *slot) {
				return !ctx->is_advertisement_compatible_with_slot(ad, slot, ad->is_watched());
			});
			slots.erase(it, slots.end());
			if (slots.empty())
			{
				ad->flags_ |= Ads_Advertisement_Candidate_Lite::FLAG_INVALID;
				if(ad->is_watched()) ctx->log_watched_info(*ad, "NO_APPLICABLE_SLOT");
				ctx->decision_info_.add_values(Decision_Info::INDEX_2);
				continue;
			}
		}

		bool test_unit_sponsorship = ctx->need_test_unit_sponsorship(*ad);
		bool enable_compliance_check = ad->is_compliance_check_enabled_;
		if(!enable_compliance_check && test_unit_sponsorship)
		{
			Ads_Slot_List &slots = ad->applicable_slots_;
			slots.erase(std::remove_if(slots.begin(), slots.end(), [ctx, ad, this](Ads_Slot_Base *slot) {
				bool excluded = !this->test_advertisement_unit_sponsorship(ad, slot, false);
				if (excluded)
				{
					ADS_DEBUG((LP_TRACE, "ad %s not in consideration in slot %s due to sponsored inventory.\n", CANDIDATE_LITE_ID_CSTR(ad), slot->custom_id_.c_str()));
					if (ad->is_watched()) ctx->log_watched_info(*ad, slot, "SPONSORED_INVENTORY");
				}
				return excluded;}), slots.end());

			if (slots.empty())
			{
				ad->flags_ |= Ads_Advertisement_Candidate_Lite::FLAG_INVALID;
				ctx->decision_info_.add_values(Decision_Info::INDEX_2);
				ctx->decision_info_.reject_ads_.put(*ad, Reject_Ads::SPONSORED_INVENTORY);
				if (Ads_Server_Config::instance()->enable_debug_)
					ctx->flattened_candidates_.erase(ad);

				ad->set_error(selection::Error_Code::EXCLUSIVITY_BY_STREAM);
				continue;
			}
		}

		std::unique_ptr<Ads_Advertisement_Candidate> candidate;
		{
			Ads_Advertisement_Candidate *candi = nullptr;
			int found = this->find_best_path_for_advertisement(ctx, ad, candi);

			if (!ad->is_two_phase_translated_ad())
			{
				Market_Ad_Ingest_Ctx_Ptr ingestion = ctx->create_market_ad_ingestion(*ad);
				if (ingestion)
					ctx->market_ad_ingestions_.emplace_back(std::move(ingestion));
			}

			if (found < 0)
			{
				ad->set_error(ad->first_selection_error());
				ctx->decision_info_.add_values(Decision_Info::INDEX_26);
				continue;
			}
			ADS_ASSERT(candi);
			candidate.reset(candi);
		}

		if (enable_compliance_check)
		{
			bool excluded_by_sponsorship;
			if (!candidate->compliance_check_for_market_ad(ctx, test_unit_sponsorship, excluded_by_sponsorship))
			{
				ad->set_error(excluded_by_sponsorship ? selection::Error_Code::EXCLUSIVITY_BY_STREAM
									: ad->last_selection_error());
				continue;
			}
		}
		selection::Error_Code error_code = selection::Error_Code::NO_ERROR;
		if (!candidate->check_compliance_for_inventory_protection(ctx, error_code))
		{
			ADS_DEBUG((LP_DEBUG, "compliance for inventory protection is failed for candidate %s, error %s\n", ADS_ENTITY_ID_CSTR(candidate->ad_id()),
							selection::Error::report_error_code_str(error_code).c_str()));
			if (error_code != selection::Error_Code::NO_ERROR)
			{
				ad->set_error(error_code);
			}
			if (ad->is_watched()) ctx->log_watched_info(*ad, "INVALID_COMPLIANCE_INFO");
			continue;
		}

		if (!candidate->slot_references_.empty())
		{
			const auto &slot_pr = *candidate->slot_references_.begin();
			const auto &slot_ref = slot_pr.second;
			if (slot_ref->access_path_.mkpl_path_ != nullptr &&
				!ctx->mkpl_executor_->test_advertisement_restriction(ctx, *slot_ref->access_path_.mkpl_path_, *ad, slot_pr.first))
				continue;
		}

		bool block_unknown = ctx->is_unknown_compliance_ad_blocked(candidate.get());
		if (!enable_compliance_check && block_unknown && !ad->test_inventory_restriction_for_unknown(ctx, &ctx->restriction_.industry_blacklist_,
					&ctx->restriction_.global_brand_blacklist_, &ctx->restriction_.global_brand_whitelist_, &ctx->restriction_.global_advertiser_blacklist_, &ctx->restriction_.global_advertiser_whitelist_))
		{
			ad->set_error(ad->last_selection_error());
			continue;
		}

#if defined(ADS_ENABLE_FORECAST)
		if (!ad->is_portfolio() && (!candidate->creatives_.empty() || ad->is_tracking()))
#endif
		{
			if (((candidate->assoc_ & ads::MEDIA_VIDEO) && !candidate->test_candidate_restriction(ctx, ads::MEDIA_VIDEO, block_unknown))
					|| ((candidate->assoc_ & ads::MEDIA_SITE_SECTION) && !candidate->test_candidate_restriction(ctx, ads::MEDIA_SITE_SECTION, block_unknown)))
			{
				candidate->assoc_ = ads::MEDIA_NONE;
				ad->set_error(ad->last_selection_error());
				ADS_DEBUG((LP_TRACE, "ad %s cannot be delivered into any slot\n", CANDIDATE_LITE_ID_CSTR(ad)));
				continue;
			}

			if(!this->test_creative_for_request_restriction(ctx, candidate.get()))
			{
				if (ad->is_watched()) ctx->log_watched_info(*ad, nullptr, "NO_CREATIVE");
				ctx->decision_info_.reject_ads_.put(*ad, Reject_Ads::NO_CREATIVES);
				ctx->decision_info_.add_values(Decision_Info::INDEX_4);
				continue;
			}
		}

		bool candidate_profile_check_passed = false;
		for (Ads_Slot_Base *slot : ad->applicable_slots_)
		{
			std::vector<selection::Error_Code> err_codes;
			/// find applicable creatives for candidate
			if (slot->flags_ & Ads_Slot_Base::FLAG_TRACKING_ONLY)
			{
				if (ad->is_tracking())
					(void)candidate->ensure_applicable_creatives(slot);
				else
				{
					candidate->non_applicable_slots_.insert(slot);
					if (ad->is_watched()) ctx->log_watched_info(*ad, slot, "TRACKING_ONLY");
				}
			}
			else if (ctx->explicit_renditions_.find(ad->id()) != ctx->explicit_renditions_.end())
			{
				std::multimap<Ads_GUID, const Ads_Creative_Rendition *>::const_iterator first = ctx->explicit_renditions_.lower_bound(ad->id()),
						last =  ctx->explicit_renditions_.upper_bound(ad->id());

				for (; first != last; ++first)
				{
					const Ads_Creative_Rendition *rendition = first->second;
					if (rendition)
					{
						if (ctx->request_->rep()->ads_.check_environment_)
						{
							if (ads::entity::is_valid_id(ad->ad_slot_term()))
							{
								if (slot->slot_terms().empty() || slot->slot_terms().find(ad->ad_slot_term()) == slot->slot_terms().end())
								{
									rendition = 0;
									if (ad->is_watched()) ctx->log_watched_info(*ad, slot, "SLOT_TYPE");
								}
							}
							selection::Error_Code err = selection::Error_Code::UNDEFINED;
							if (rendition && !this->is_creative_applicable_for_slot(ctx, candidate.get(), rendition, slot, false, &err))
							{
								rendition = 0;
								err_codes.push_back(err);
							}
						}
					}

					if (rendition)
						candidate->applicable_creatives(slot).push_back(rendition);
				}

				if (candidate->applicable_creatives(slot).empty())
				{
					if (ad->is_watched()) ctx->log_watched_info(*ad, slot, "NO_EXPLICIT_CREATIVE");
					candidate->non_applicable_slots_.insert(slot);
				}
			}
			else
#if defined(ADS_ENABLE_FORECAST)
			if (!(ad->is_portfolio() && (ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::BYPASS_RESTRICTION)))
#endif
			{
				profile_matching(ctx, candidate.get(), slot, &err_codes);
			}
			if (!candidate->applicable_creatives(slot).empty())
			{
				candidate_profile_check_passed = true;
			}
			else
			{
				for (auto err : err_codes)
				{
					ad->log_selection_error(err, slot, selection::Error_Code::PROFILE_CHECK_FAILED);
				}
				ctx->add_restricted_candidate_log(ad->id(), "PROFILE_MATCHING_FAILED");
			}
		}

		if (!candidate_profile_check_passed)
		{
			ctx->decision_info_.add_values(Decision_Info::INDEX_4);
			ad->set_error(selection::Error_Code::PROFILE_CHECK_FAILED);
		}
		else
			ad->flags_ |= Ads_Advertisement_Candidate_Lite::FLAG_PROFILE_MATCHING_PASS;

#if !defined(ADS_ENABLE_FORECAST)
		ctx->final_candidates_.push_back(candidate.release());
#else
		if (ctx->request_flags() & Ads_Request::Smart_Rep::FORCE_PROPOSAL)
			candidate->flags_ |= Ads_Advertisement_Candidate::FLAG_PROPOSAL;

		const bool failed_on_rating = (candidate->creatives_.empty() && !ad->is_tracking());
		if (ad->is_portfolio() || failed_on_rating)
		{
			if (ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::BYPASS_RESTRICTION)
				ctx->final_candidates_.push_back(candidate.release());
		}
		else if (candidate->meet_schedule())
		{
			if (ctx->request_flags() & Ads_Request::Smart_Rep::BYPASS_BUDGET_CHECK)
				ctx->final_candidates_.push_back(candidate.release());
			else if (ctx->request_flags() & Ads_Request::Smart_Rep::PROPOSAL_IF_NO_BUDGET)
			{
				if (candidate->meet_ext_schedule()) candidate->flags_ |= Ads_Advertisement_Candidate::FLAG_PROPOSAL;
				ctx->final_candidates_.push_back(candidate.release());
			}
			else if (!candidate->meet_ext_schedule()) // in READ scenario
				ctx->final_candidates_.push_back(candidate.release());
		}
		else
			ctx->final_candidates_.push_back(candidate.release());

		if (candidate)
		{
			ADS_DEBUG((LP_TRACE, "%s cannot be delivered\n",  ad->get_ad_type_and_id().c_str()));
			continue;
		}
#endif
	}

	if (Ads_Server_Config::instance()->enable_cch_client_)
	{
		this->collect_jit_ingestions(ctx);
	}

	ctx->decision_info_.set_values(Decision_Info::INDEX_11, static_cast<int>(ctx->final_candidates_.size()));

	if (Ads_Server_Config::instance()->enable_debug_)
	{
		ADS_DEBUG((LP_TRACE,
		           "final candidate ad's  (total %d): \n", ctx->final_candidates_.size()));
		std::for_each(ctx->final_candidates_.begin(), ctx->final_candidates_.end(), std::mem_fun(&Ads_Advertisement_Candidate::dump));
		ADS_DEBUG((LP_TRACE, "\n"));
	}

	if (ctx->listeners_)
		this->notify_selection_listeners(ctx, Ads_Selection_Listener::STAGE_8);
	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_8_1);


// __MKPL_PREPARE_FOR_FILLING_SLOTS:

////////////////////////////////////////////////////////////////////////////////
//
// Phase 9. Filling advertisement slots
//
//////////////////////////////////////////////////////////////////////////////////

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG0((LP_TRACE,
		            "Ads_Selector::select: ------------------------------------------------------------\n"
		            "Ads_Selector::select:                PHASE 9: Filling advertisement slots         \n"
		            "Ads_Selector::select: ------------------------------------------------------------\n"
		           ));
	}
	{
	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_9_0);
	ctx->stage_ = 9;

	bool need_load_mkpl_bridge_ad = ctx->is_mkpl_bridge_url_enabled() && ctx->has_external_bridge_ad();
	if (ctx->scheduler_->is_live_schedule() || need_load_mkpl_bridge_ad)
	{
		ctx->scheduler_->pre_process_load_candidates(ctx->request_flags(), ctx->video_slots_);
		ctx->scheduler_->load_candidates(ctx, need_load_mkpl_bridge_ad);
		ctx->scheduler_->post_process_load_candidates(ctx->video_slots_);
	}

	if (!(ctx->request_flags() & Ads_Request::Smart_Rep::DONT_SHUFFLE_CANDIDATES))
	{
#if !defined(ADS_ENABLE_FORECAST)
		std::random_shuffle(ctx->final_candidates_.begin(), ctx->final_candidates_.end());
#else
/*
		if (ctx->request_flags() & Ads_Request::Smart_Rep::BYPASS_RESTRICTION) //in scenario[UNCONSTAINED]
		{
			std::random_shuffle(ctx->final_candidates_.begin(), ctx->final_candidates_.end());//forecast shuffle only in scenario[UNCONSTAINED]
			ctx->sticky_candidates_.clear();
			for (Ads_Advertisement_Candidate_List::const_iterator it = ctx->final_candidates_.begin(); it != ctx->final_candidates_.end(); ++it)
			{
				const Ads_Advertisement_Candidate *candidate = *it;
				ctx->sticky_candidates_.push_back(candidate->ad_->id_);
			}
			//af may skip some draft ads in uga round and we want such ad to be delivered in real round
			//but if the candidate is not in sticky candidates, it will be skipped
			//code below is to append the diff of flattened_candidates_saved_ and sticky_candidates_ to the end of sticky_candidates_.end
			//so the ad discarded in uga round can appear in real round
			for (auto fit = ctx->flattened_candidates_saved_.begin(); fit != ctx->flattened_candidates_saved_.end(); ++fit)
			{
				bool find_sticky_candidate = false;
				for (auto sit = ctx->sticky_candidates_.begin(); sit != ctx->sticky_candidates_.end(); ++sit)
				{
					if (*sit == *fit)
						find_sticky_candidate = true;
				}
				if (!find_sticky_candidate)
					ctx->sticky_candidates_.push_back(*fit);
			}
			ADS_DEBUG0((LP_TRACE, "forecast sticky candidates: %s \n", ads::entities_to_str(ctx->sticky_candidates_).c_str()));
		}
		else
		{
			std::multimap<Ads_GUID, Ads_Advertisement_Candidate*> final_candidates;//index by id only
			for (Ads_Advertisement_Candidate_List::const_iterator it = ctx->final_candidates_.begin(); it != ctx->final_candidates_.end(); ++it)
			{
				Ads_Advertisement_Candidate *candidate = *it;
				final_candidates.emplace(candidate->ad_->id_, candidate);
			}
			ctx->final_candidates_.clear();
			for (Ads_GUID_List::const_iterator ait = ctx->sticky_candidates_.begin(); ait != ctx->sticky_candidates_.end(); ++ait)
			{
				auto it = final_candidates.find(*ait);
				if (it != final_candidates.end())
				{
					ctx->final_candidates_.push_back(it->second);
					final_candidates.erase(it);
				}
			}
			ads::for_each2nd(final_candidates.begin(), final_candidates.end(), std::mem_fun(&Ads_Advertisement_Candidate::destroy));//destory those never show up in sticky ads
		}
*/
#endif
	}

    //TODO: refactor prepare companion
	if (!ctx->request_->has_explicit_candidates() || ctx->request_->rep()->ads_.check_companion_)
	{
		this->prepare_for_companion_validation(ctx, ctx->final_candidates_);
	}

	if (ctx->scheduler_->has_schedule())
		ctx->scheduler_->fill_scheduled_candidates(*ctx, ctx->video_slots_, ctx->selected_candidates_);
	if (ctx->scheduler_->is_live_schedule())
		ctx->scheduler_->post_process_fill_scheduled_candidates(*ctx, ctx->video_slots_, ctx->final_candidates_, ctx->errors_);

	/// using the shuffle random
	if (ctx->log_competing_ad_score_)
	{
		for (Ads_Advertisement_Candidate_List::const_iterator it = ctx->final_candidates_.begin();
			it != ctx->final_candidates_.end(); ++it)
		{
			Ads_Advertisement_Candidate *candidate = *it;
			auto *closure = candidate->owner_.to_closure();
			if (closure
				&& closure->num_competing_ad_score_ > 0
				&& !candidate->ad_->is_sponsorship()
				&& !candidate->ad_->is_exempt()
				&& !candidate->ad_->is_house_ad(-1)
				&& !candidate->slot_references_.empty()
				&& candidate->ad_->competing_eCPM_ < 0
				)
			{
				ctx->scoring_candidates_.push_back(candidate);
				--closure->num_competing_ad_score_;
			}
		}
	}

	Ads_Advertisement_Candidate::greater video_ad_greater(ctx, ads::MEDIA_VIDEO);
	if (ctx->root_asset_)
	{
		apply_reseller_optimized_ad_ranking(ctx, ads::MEDIA_VIDEO);
		std::stable_sort(ctx->final_candidates_.begin(), ctx->final_candidates_.end(), video_ad_greater);

		if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
		{
			dump_candidate_slots_assignment_info(*ctx, ads::MEDIA_VIDEO);
		}
	}

	{
		for (auto *candidate : ctx->final_candidates_)
		{
			const Ads_Deal* deal = nullptr;
			if (candidate->operation_owner().unified_yield_.is_enabled_ &&
					candidate->lite_ && (deal = candidate->lite_->get_deal()) && deal->type_ == Ads_Deal::DEAL)
				candidate->operation_owner().unified_yield_.unified_yield_candidates_.push_back(candidate);
		}
	}

	ctx->mkpl_executor_->prepare_for_filling_slots(ctx, video_ad_greater);

	if (ctx->root_asset_ && ctx->tracking_slot_)
	{
		Ads_Slot_Base *slot = ctx->tracking_slot_;
		int64_t profit = 0;
		while (true)
		{
			size_t flags = 0;
			Ads_Advertisement_Candidate *candidate = 0;
			if (this->find_advertisement_for_slot(ctx, slot, ctx->final_candidates_, ctx->selected_candidates_, candidate, flags) < 0 || !candidate)
				break;

			candidate->slot_ = slot;
			slot->ordered_advertisements_.push_back(candidate);

			ctx->selected_candidates_.insert(candidate);

			profit += candidate->effective_eCPM(slot);
		}

		ctx->profit_ += profit;
	}

#if defined(ADS_ENABLE_FORECAST)
	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_9_0_1);
#endif

	Ads_Advertisement_Candidate_List paying_ads, house_ads;
	bool try_house_ads = true;
	if (ctx->root_asset_)
	{
		/// split paying ads and house ads
		if (ctx->repeat_paying_ad_before_house_ad())
		{
			std::set<Ads_GUID> plcs;//reseller placements avilable via HG or YT mrm rule
			for (Ads_Advertisement_Candidate_List::iterator it = ctx->final_candidates_.begin();
				 it != ctx->final_candidates_.end(); ++it)
			{
				Ads_Advertisement_Candidate *candidate = *it;
				Ads_GUID companion_id = candidate->ad_->is_old_external() ? candidate->ad_->companion_id_ : candidate->ad_->placement_id();

				const Ads_MRM_Rule_Path *path = nullptr;
				const Ads_Slot_Base *slot = nullptr;
				double *priority = nullptr;

				// find the MRM rule path from CRO network to candidate's network and corresponding slot
				for (const auto &mp : candidate->slot_references_)
				{
					const Ads_Advertisement_Candidate::Slot_Ref_Ptr slot_ref = mp.second;
					if (mp.first->associate_type() == ads::MEDIA_VIDEO)
					{
						if (priority == nullptr || priority[0] < slot_ref->priority_[0])
						{
							priority = slot_ref->priority_;
							path = slot_ref->access_path_.mrm_rule_path_;
							slot = mp.first;
						}
					}
				}

				// For example, CRO ->Reseller1(HG w/o) -> Reseller 2(HG w/o) -> Reseller 3 (OC), equivalent_cro_network_id = Reseller 2's network id.
				Ads_GUID equivalent_cro_network_id = ctx->root_asset_->network_id_;
				bool all_edges_HG_rule = true;	// Whether the rules in the rule-path from CRO to current Reseller are all HG w/o passback.
				Ads_MRM_Rule::PRIORITY rule_priority = Ads_MRM_Rule::PRIORITY_NONE; // priority of first non HG w/o passback rule
				if (path != nullptr)
				{
					for (const auto &edge : path->edges_)
					{
						const Ads_MRM_Rule *rule = edge.rule_;
						ADS_ASSERT(rule != nullptr);
						if (rule->priority_ != Ads_MRM_Rule::HARD_GUARANTEED) // HARD_GUARANTEED means HG w/o Passback
						{
							equivalent_cro_network_id = rule->network_id_;
							all_edges_HG_rule = false;
							rule_priority = rule->priority_;
							break;
						}
					}
				}

				//If the candidate is from a LW Reseller, must consider the last rule, filter PG deal
				if (all_edges_HG_rule && candidate->ad_->is_external_ && !candidate->is_guaranteed_deal_ad())
				{
					const Ads_MRM_Rule *external_rule = candidate->external_rule(slot);
					if (external_rule != nullptr)
					{
						if (external_rule->priority_ != Ads_MRM_Rule::HARD_GUARANTEED)
						{
							equivalent_cro_network_id = external_rule->network_id_;
							all_edges_HG_rule = false;
							rule_priority = external_rule->priority_;
						}
					}
				}

				if (all_edges_HG_rule)
				{
					equivalent_cro_network_id = candidate->ad_->network_id_;
				}

				if (candidate->is_companion() && plcs.find(companion_id) != plcs.end())
				{
					paying_ads.push_back(candidate);
				}
				else if (!all_edges_HG_rule && rule_priority == Ads_MRM_Rule::YOU_FIRST) /* YOU_FIRST means HG w/ Passback */
				{
					paying_ads.push_back(candidate);
					plcs.insert(companion_id);
				}
				else
				{
					if ((all_edges_HG_rule && candidate->ad_->is_house_ad(equivalent_cro_network_id))
						|| (!all_edges_HG_rule && candidate->competing_eCPM(ads::MEDIA_VIDEO) <= 0))
					{
						house_ads.push_back(candidate);
					}
					else
					{
						paying_ads.push_back(candidate);
						plcs.insert(companion_id);
					}
				}
			}

			if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
			{
				ADS_DEBUG0((LP_TRACE, "Paying Ads:\n"));
				for (const auto &candidate : paying_ads)
				{
					ADS_DEBUG0((LP_TRACE, "    %d:%s,", candidate->ad_->network_id_, CANDIDATE_LITE_LPID_CSTR(candidate->lite_)));
				}
				ADS_DEBUG0((LP_TRACE, "\n"));

				ADS_DEBUG0((LP_TRACE, "Non-Paying Ads:\n"));
				for (const auto &candidate : house_ads)
				{
					ADS_DEBUG0((LP_TRACE, "    %d:%s,", candidate->ad_->network_id_, CANDIDATE_LITE_LPID_CSTR(candidate->lite_)));
				}
				ADS_DEBUG0((LP_TRACE, "\n"));
	 		}
		}

		Ads_Advertisement_Candidate_List& candidates =
			(ctx->root_asset_ && ctx->repeat_paying_ad_before_house_ad()) ? paying_ads : ctx->final_candidates_;

		if (!ctx->video_slots_.empty())
		{
			int64_t profit = 0;
			this->fill_advertising_slots_v(ctx, candidates, ctx->video_slots_, ctx->selected_candidates_, profit);
			ctx->profit_ += profit;

			/// to make sure at least one temporal ads is filled first
			if (ctx->selected_candidates_.empty() && !house_ads.empty())
			{
				try_house_ads = false;

				profit = 0;
				this->fill_advertising_slots_v(ctx, house_ads, ctx->video_slots_, ctx->selected_candidates_, profit);
				ctx->profit_ += profit;
			}

			for (Ads_Advertisement_Candidate_Set::const_iterator candidate_it = ctx->selected_candidates_.begin(); candidate_it != ctx->selected_candidates_.end(); ++candidate_it)
			{
#if defined (ADS_ENABLE_FORECAST)
				if (!(*candidate_it)->is_proposal())
#else
				if ((*candidate_it)->ad_unit()->is_temporal())
#endif
				{
					ctx->status_ |= STATUS_HAS_TEMPORAL_AD;
					break;
				}
			}
		}

		if (!ctx->player_slots_.empty())
		{
			int64_t profit = 0;
			this->fill_advertising_slots_p(ctx, ctx->final_candidates_, ctx->player_slots_, ctx->selected_candidates_, profit);
			ctx->profit_ += profit;
		}
	}

	if (ctx->root_section_)
	{
		apply_reseller_optimized_ad_ranking(ctx, ads::MEDIA_SITE_SECTION);
		std::stable_sort(ctx->final_candidates_.begin(), ctx->final_candidates_.end(), Ads_Advertisement_Candidate::greater(ctx, ads::MEDIA_SITE_SECTION));
		if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
		{
			dump_candidate_slots_assignment_info(*ctx, ads::MEDIA_SITE_SECTION);
		}
	}

#if defined(ADS_ENABLE_FORECAST)
	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_9_0_1);
	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_9_0_2);
#endif

	if (ctx->root_section_ && !ctx->page_slots_.empty())
	{
		int64_t profit = 0;
		this->fill_advertising_slots_p(ctx, ctx->final_candidates_, ctx->page_slots_, ctx->selected_candidates_, profit);
		ctx->profit_ += profit;
	}

#if defined(ADS_ENABLE_FORECAST)
	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_9_0_2);
	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_9_0_3);
#endif
	if (ctx->scheduler_->is_live_schedule())
	{
		if (ctx->scheduler_->need_replace_scheduled_ad())
		{
			ctx->scheduler_->pre_process_preempt_scheduled_candidates(*ctx);
			int64_t profit = 0;
			ctx->scheduler_->preempt_scheduled_candidates(*ctx, ctx->selected_candidates_, ctx->final_candidates_, profit);
			ctx->profit_ += profit;
			ctx->scheduler_->post_process_preempt_scheduled_candidates(ctx->final_candidates_);
		}
		ctx->scheduler_->set_endpoint_action_for_ads();
	}
	ADS_DEBUG((LP_TRACE, "Previous Final Advertising Plan: \n"));

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		for (size_t i = 0; i < ctx->all_final_candidates_.size(); ++i)
		{
			ADS_DEBUG0((LP_TRACE, "    %s:%s,"
					, CANDIDATE_LITE_LPID_CSTR(ctx->all_final_candidates_[i]->lite_)
					, ctx->all_final_candidates_[i]->slot_ ? ctx->all_final_candidates_[i]->slot_->custom_id_.c_str(): "N/A"));
		}
		ADS_DEBUG0((LP_TRACE, "\n\n"));
	}

	ADS_DEBUG((LP_TRACE, "Current accumulated eCPM=%s\n", ads::i64_to_str(ctx->profit_).c_str()));
	ADS_DEBUG((LP_TRACE, "Current Round Final Advertising Plan:\n"));

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		for (size_t i = 0; i < ctx->final_candidates_.size(); ++i)
		{
			ADS_DEBUG0((LP_TRACE, "    %s:%s,"
					, CANDIDATE_LITE_LPID_CSTR(ctx->final_candidates_[i]->lite_)
					, ctx->final_candidates_[i]->slot_ ? ctx->final_candidates_[i]->slot_->custom_id_.c_str(): "N/A"));
		}
		ADS_DEBUG0((LP_TRACE, "\n\n"));
	}

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_ && !ctx->unified_yield_filled_candidates_.empty())
	{
		ADS_DEBUG((LP_DEBUG, "unified yield v2 filled uplift programmatic ad:\n"));
		for (const auto *candidate : ctx->unified_yield_filled_candidates_)
		{
			ADS_DEBUG0((LP_DEBUG, "    %s:%s,"
					, CANDIDATE_LITE_LPID_CSTR(candidate->lite_)
					, candidate->slot_ ? candidate->slot_->custom_id_.c_str(): "N/A"));
		}
		ADS_DEBUG0((LP_DEBUG, "\n\n"));
	}

	if (try_house_ads)
	{
		if (ctx->pending_house_ads_.empty())
			ctx->pending_house_ads_.swap(house_ads);
		else
			ctx->pending_house_ads_.insert(ctx->pending_house_ads_.end(), house_ads.begin(), house_ads.end());
	}

	if (this->prepare_next_round_selection(ctx) > 0)
	{
#if defined(ADS_ENABLE_FORECAST)
		ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_9_0_3);
#endif
		ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_9_0);
		ctx->flags_ |= Ads_Selection_Context::FLAG_NEXT_ROUND_SELECTION_REQUIRED;
		return 2;
	}

	Ads_Monitor::stats_observe(Ads_Monitor::TX_SELECTION_FLATTENED_CANDIDATES_NUM, ctx->all_flattened_candidates_.size());

#if defined(ADS_ENABLE_FORECAST)
	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_9_0_3);
	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_9_0_4);
#endif

	if (!ctx->selected_candidates_.empty())
	{
		Ads_Advertisement_Candidate_List candidates(ctx->selected_candidates_.begin(), ctx->selected_candidates_.end());

		// sort for QA
		if (ctx->request_flags() & Ads_Request::Smart_Rep::DONT_SHUFFLE_CANDIDATES)
			std::stable_sort(candidates.begin(), candidates.end(), Ads_Advertisement_Candidate::greater(ctx, ads::MEDIA_VIDEO, false, true, true, true /* only compare id */));

#if defined(ADS_ENABLE_FORECAST)
		Take_Advertisement_Slot_Position* slot_position = new Take_Advertisement_Slot_Position();
		std::vector<Ads_Advertisement_Candidate *> tmp;
		for (Ads_Advertisement_Candidate_List::const_iterator it = candidates.begin(); it != candidates.end(); ++it)
			tmp.push_back(*it);
		std::stable_sort(tmp.begin(), tmp.end(), Ads_Advertisement_Candidate::greater(ctx, ads::MEDIA_VIDEO, false /*ignore_duration*/, true /*ignore_active*/));
		Take_Advertisement_Candidate_Rank* candidate_rank = new Take_Advertisement_Candidate_Rank();
		for (size_t i = 0; i < tmp.size(); ++i)
			(*candidate_rank)[tmp[i]] = tmp.size() - i;
#endif

		for (Ads_Advertisement_Candidate_List::iterator it = candidates.begin(); it != candidates.end(); ++it)
		{
			Ads_Advertisement_Candidate *candidate = *it;
			if (candidate == nullptr)
				continue;

			// scheduled ad and dynamic replacer ad's ref is taken during filling ad
			if (candidate->is_scheduled_or_replacer())
				continue;

#if defined(ADS_ENABLE_FORECAST)
			this->transfer_selected_candidate_advertisements(ctx, candidate, ctx->delivered_candidates_, slot_position, candidate_rank);
#else
			this->transfer_selected_candidate_advertisements(ctx, candidate, ctx->delivered_candidates_);
#endif
		}

		/// multiple instances of an ad
		if (!ctx->request_->rep()->capabilities_.disable_ad_unit_in_multiple_slots_)
#if defined(ADS_ENABLE_FORECAST)
			this->try_replicate_selected_candidate_advertisements(ctx, candidates, ctx->delivered_candidates_, slot_position, candidate_rank);
		if (slot_position) delete slot_position;
		if (candidate_rank) delete candidate_rank;
#else
			this->try_replicate_selected_candidate_advertisements(ctx, candidates, ctx->delivered_candidates_);
#endif
	}

	ctx->is_unified_yiled_enabled_ = false;
	ctx->pending_house_ads_.insert(ctx->pending_house_ads_.begin(),
			ctx->unified_yield_replaced_soft_guaranteed_candidates_.begin(), ctx->unified_yield_replaced_soft_guaranteed_candidates_.end());

	/// FDB-4115
	if (!ctx->pending_house_ads_.empty())
	{
		int64_t profit = 0;
		this->knapsack_initialize_solution(ctx, ctx->pending_house_ads_, ctx->video_slots_, ctx->selected_candidates_, 0, profit);
		ctx->profit_ += profit;

		if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_ && !ctx->unified_yield_replaced_soft_guaranteed_candidates_.empty())
		{
			ADS_DEBUG((LP_DEBUG, "unified yield v2 soft guaranteed ad:\n"));
			for (const auto *candidate : ctx->unified_yield_replaced_soft_guaranteed_candidates_)
			{
				ADS_DEBUG0((LP_DEBUG, "    %s:%s,"
						, CANDIDATE_LITE_LPID_CSTR(candidate->lite_)
						, candidate->slot_ ? candidate->slot_->custom_id_.c_str(): "N/A"));
			}
			ADS_DEBUG0((LP_DEBUG, "\n\n"));
		}

		Ads_Advertisement_Candidate_List selected_house_ads;
		for (Ads_Advertisement_Candidate* candidate : ctx->pending_house_ads_)
		{
			if (ctx->selected_candidates_.find(candidate) != ctx->selected_candidates_.end() && candidate->slot_)
				selected_house_ads.push_back(candidate);
		}

		// sort for QA
		if (ctx->request_flags() & Ads_Request::Smart_Rep::DONT_SHUFFLE_CANDIDATES)
			std::stable_sort(selected_house_ads.begin(), selected_house_ads.end(), Ads_Advertisement_Candidate::greater(ctx, ads::MEDIA_VIDEO, false, true, true, true /* only compare id */));

		for (Ads_Advertisement_Candidate_List::iterator it = selected_house_ads.begin(); it != selected_house_ads.end(); ++it)
		{
			Ads_Advertisement_Candidate *candidate = *it;

			if (candidate == nullptr)
				continue;

			// scheduled ad and dynamic replacer ad's ref is taken during filling ad
			if (candidate->is_scheduled_or_replacer())
				continue;

			this->transfer_selected_candidate_advertisements(ctx, candidate, ctx->delivered_candidates_);
		}

		/// multiple instances of an ad
		if (!ctx->request_->rep()->capabilities_.disable_ad_unit_in_multiple_slots_)
			this->try_replicate_selected_candidate_advertisements(ctx, selected_house_ads, ctx->delivered_candidates_);
	}

	{
		Ads_Advertisement_Candidate_Ref *leader = 0;
		const Ads_Slot_Base *previous_slot = 0;
		Ads_Advertisement_Candidate_Ref_List new_delivered_candidates;

		for (Ads_Advertisement_Candidate_Ref_List::iterator it = ctx->delivered_candidates_.begin(); it != ctx->delivered_candidates_.end(); ++it)
		{
			Ads_Advertisement_Candidate_Ref *ref = *it;
			Ads_Advertisement_Candidate *candidate = const_cast<Ads_Advertisement_Candidate *>(ref->candidate_);
			const Ads_Advertisement *ad = candidate->ad_;

			if (!ref->slot_ || ref->slot_->env() != ads::ENV_VIDEO)
			{
				new_delivered_candidates.push_back(ref);
				continue;
			}

			Ads_Slot_Base *slot = (reinterpret_cast<const Ads_Video_Slot *>(ref->slot_)->parent_ ? reinterpret_cast<const Ads_Video_Slot *>(ref->slot_)->parent_ : ref->slot_);
			Ads_Video_Slot* vslot = reinterpret_cast<Ads_Video_Slot *>(ref->slot_);

			if (candidate->non_applicable_slots_.find(vslot) != candidate->non_applicable_slots_.end())
			{
				candidate->used_slots_.erase(vslot);
				Ads_Advertisement_Candidate_Ref_List::iterator rit = std::find(candidate->references_.begin(), candidate->references_.end(), ref);
				if (rit != candidate->references_.end()) candidate->references_.erase(rit);
				ref->destroy();
				continue;
			}

			if (ref->is_companion_) goto copy_and_continue;
			if (ref->is_fallback_) goto copy_and_continue;
			if (ref->candidate_->ad_->is_bumper()) goto copy_and_continue;

			if (vslot->time_position_class_ == "preroll" || vslot->time_position_class_ == "midroll" || vslot->time_position_class_ == "postroll")
			{
				if (previous_slot != slot)
				{
					leader = 0;
					previous_slot = slot;
				}

				if (ad->is_leader())
					leader = ref;

				goto copy_and_continue;
			}
			else if (vslot->time_position_class_ == "overlay" || vslot->time_position_class_ == "pause_midroll")
			{
				// no leader found
				if (!leader) goto copy_and_continue;

				if (!ref->candidate_->ad_->is_follower()) goto copy_and_continue;

				// already suitable follower
				if (ref->candidate_->ad_->placement_id() == leader->candidate_->ad_->placement_id()) goto copy_and_continue;

				// try to swap in most suitable follower
				if ((!ad->companion_ || ad->companion_when_possible_ || (ad->advanced_companion_ && !ad->ad_linking_groups_.empty() && ad->ad_linking_groups_[0].second != Ads_Advertisement::ALL_LINKED) || candidate->references_.size() > 1) && (!(ctx->request_flags() & Ads_Request::Smart_Rep::FORCE_PROPOSAL)))
				{
					bool swapped = false;

					for (Ads_Advertisement_Candidate *follower : ctx->all_final_candidates_)
					{
						if (!follower->ad_->is_follower()
							|| follower->ad_->placement_id() != leader->candidate_->ad_->placement_id())
							continue;

						if (follower->used_slots_.find(vslot) != follower->used_slots_.end()) continue;

						if (follower->duration(vslot) > vslot->duration_remain() + candidate->duration(vslot))
							continue;

						if (!this->test_advertisement_slot_restrictions(ctx, follower, vslot, 0, true))
							continue;

						if (!this->test_advertisement_unit_sponsorship(follower->lite_, vslot, true))
							continue;

						vslot->remove_advertisement(ctx, candidate);
						vslot->take_advertisement(ctx, follower);

						Ads_Advertisement_Candidate_Ref *fref = new Ads_Advertisement_Candidate_Ref(follower, vslot);
						fref->replica_id_ = follower->references_.size();
						follower->used_slots_.insert(vslot);
						follower->references_.push_back(fref);
						this->calculate_effective_inventory(ctx, fref);

						candidate->used_slots_.erase(vslot);
						Ads_Advertisement_Candidate_Ref_List::iterator rit = std::find(candidate->references_.begin(), candidate->references_.end(), ref);
						if (rit != candidate->references_.end()) candidate->references_.erase(rit);
						ref->destroy();

						new_delivered_candidates.push_back(fref);
						swapped = true;
						break;
					}

					// swap sucessfully
					if (swapped)
						continue;
				}
			}
		copy_and_continue:
			new_delivered_candidates.push_back(ref);
		}

		ctx->delivered_candidates_.swap(new_delivered_candidates);
	}

#if defined(ADS_ENABLE_FORECAST)
	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_9_0_4);
#endif

	if (Ads_Server_Config::instance()->enable_debug_)
	{
		if (ctx->root_asset_ && ctx->repeat_paying_ad_before_house_ad())
		{
			if (!ctx->pending_house_ads_.empty())
				ctx->final_candidates_tagged_.push_back(std::make_pair("house_ads", ctx->pending_house_ads_));
			if (!paying_ads.empty())
				ctx->final_candidates_tagged_.push_back(std::make_pair("paying_ads", paying_ads));
		}
	}

	this->calculate_advertisement_position_in_slot(ctx, ctx->delivered_candidates_);
	ctx->mkpl_executor_->finalize_true_avails_slot_usages(ctx);

	ctx->find_competing_ads_for_soft_guaranteed_ads();

#if defined(ADS_ENABLE_FORECAST)
	//append fallback ads in na/real.
	if (!(ctx->request_flags() & Ads_Request::Smart_Rep::FORCE_PROPOSAL) &&
		((ctx->root_asset_ && ctx->fallback_enabled_networks_.count(ctx->root_asset_->network_id_) > 0) || ctx->fallback_enabled_networks_.count(-1) > 0) )
#endif
	if (ctx->supports_fallback_ad())
	{
		this->append_fallback_candidate_advertisements(ctx, ctx->all_final_candidates_, ctx->delivered_candidates_);
	}

	ctx->decision_info_.set_values(Decision_Info::INDEX_24, static_cast<int>(ctx->delivered_candidates_.size()));
	// log watched untargeted orders
	if (!ctx->watched_orders_.empty())
	{
		Ads_GUID_Set untargetable_order_ids;
		std::set_difference(ctx->watched_orders_.begin(), ctx->watched_orders_.end(),
					ctx->mkpl_executor_->all_targetable_order_ids().begin(),
					ctx->mkpl_executor_->all_targetable_order_ids().end(),
					std::insert_iterator<Ads_GUID_Set>(untargetable_order_ids, untargetable_order_ids.begin()));
		for (Ads_GUID order_id : untargetable_order_ids)
		{
			const Ads_MKPL_Order *order = nullptr;
			if (ctx->repository_->find_mkpl_order(order_id, order) >= 0 && order != nullptr)
			{
				if (ctx->mkpl_executor_->is_executed_network(order->seller_network_id_))
					ctx->log_watched_info(*order, "MKPL_ORDER_NOT_MET_TARGETING");
				else
					ctx->log_watched_info(*order, "UPSTREAM_SELLER_NOT_COLLECTED");
			}
		}
	}
	///log watched untargeted ads
	if (!ctx->watched_candidates_.empty())
	{
		Ads_GUID_Set tmp0, tmp1;
		std::set_difference(ctx->watched_candidates_.begin(), ctx->watched_candidates_.end(),
					ctx->all_flattened_candidates_.begin(), ctx->all_flattened_candidates_.end(),
	                    		std::insert_iterator<Ads_GUID_Set>(tmp0, tmp0.begin()),
					selection::Loop_GUID_Compare<true>());

		std::set_difference(tmp0.begin(), tmp0.end(),
					ctx->initial_candidates_.begin(), ctx->initial_candidates_.end(),
		                	std::insert_iterator<Ads_GUID_Set>(tmp1, tmp1.begin()),
					selection::Loop_GUID_Compare<true>());

		for (Ads_GUID ad_id : tmp1)
		{
			const Ads_Advertisement *ad = nullptr;
			if (ctx->find_advertisement(ad_id, ad) < 0 || !ad || !ad->targeting_criteria_) continue;

			Ads_Asset_Section_Closure_Map::iterator cit = ctx->closures_.find(ad->network_id_);
			if (cit == ctx->closures_.end()) continue;

			Ads_Asset_Section_Closure * closure = cit->second;
			if (!closure->is_active())
			{
				ctx->log_watched_info(*ad, "NO_SLOT_ASSINGED_THROUGH_MRM");
				continue;
			}

			Ads_GUID_Set terms(closure->terms_);
			if (!this->test_targeting_criteria(ctx->repository_, ctx->delta_repository_, terms, ad->targeting_criteria_))
			{
				ctx->log_watched_info(*ad, "TARGETING_NOT_MET");
			}
		}
	}

	//MRM-24750
	for (Ads_GUID ad_id : ctx->watched_candidates_)
	{
		const Ads_Advertisement *ad = nullptr;
		if (ctx->find_advertisement(ad_id, ad) < 0 || ad == nullptr) continue;

		if (ctx->closures_.find(ad->network_id_) == ctx->closures_.end())
		{
			ctx->log_watched_info(*ad, "AD_OUT_OF_CLOSURE");
			ADS_DEBUG((LP_TRACE, "ad %s is out of closure\n", ADS_ENTITY_ID_CSTR(ad_id)));
		}

		improve_final_candidate_diagnostic_info(ad_id, *ctx);
	}

	if (ctx->listeners_)
		this->notify_selection_listeners(ctx, Ads_Selection_Listener::STAGE_9);
	}
	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_9_0);

////////////////////////////////////////////////////////////////////////////////
//
// Phase 10. Generate response
//
//////////////////////////////////////////////////////////////////////////////////
#if defined(ADS_ENABLE_FORECAST)
	if (ctx->repository_->system_config().is_clearing_price_calculator_enabled_ && !(ctx->request_flags() & Ads_Request::Smart_Rep::SKIP_GENERATE_RESPONSE))
#endif
	{
		// Clearing price calculator
		ctx->clearing_price_calculator_.reset(new selection::Clearing_Price_Calculator(ctx));
		ctx->clearing_price_calculator_->collect_candidates();
		ctx->clearing_price_calculator_->calculate();
	}

#if !defined(ADS_ENABLE_FORECAST)
	// Clearing price calculator for private exchange
	ctx->clearing_price_calculator_px_.reset(new selection::Clearing_Price_Calculator_PX(ctx));
	ctx->clearing_price_calculator_px_->collect_candidates();
	if (ctx->clearing_price_calculator_px_->need_reschedule())
	{
		ctx->flags_ |= Ads_Selection_Context::FLAG_NEEDS_EXTERNAL_NOTIFICATIONS;
		ALOG(LP_DEBUG, "External candidate notification is needed and jump out of ad selection\n");
		try_reschedule_selection(ctx);
		return 1;
	}
#else
	if (!(ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::SKIP_GENERATE_RESPONSE))
	{
		for (auto *candidate_ref : ctx->delivered_candidates_)
		{
			auto *lite = candidate_ref->candidate_->lite_;
			const auto &ext_info = lite->external_ad_info();

			if (ext_info.is_sa_playlist_failed_)
			{
				lite->flags_ |= Ads_Advertisement_Candidate_Lite::FLAG_INVALID | Ads_Advertisement_Candidate_Lite::FLAG_PLAYLIST_ACK_FAILED;
				ADS_DEBUG((LP_DEBUG, "playlist error for %s", ADS_ENTITY_ID_CSTR(lite->id())));
			}
			else if (candidate_ref->is_two_phase_translated_ad() && ext_info.is_second_phase_translation_failed_)
			{
				candidate_ref->external_ad_ = Ads_Advertisement_Candidate_Ref::external_no_ad();
				ADS_DEBUG((LP_DEBUG, "two phase translated fail for %s", ADS_ENTITY_ID_CSTR(lite->id())));
			}
		}
	}
#endif

__STAGE_10:

	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_10_0);
#if defined(ADS_ENABLE_FORECAST)
	if (ctx->request_flags() & Ads_Request::Smart_Rep::SKIP_GENERATE_RESPONSE)
	{
		goto __LOG_REQUEST;
	}
#endif

#if !defined(ADS_ENABLE_FORECAST)
	// sanity_check does not support scheduled ad checking yet
	if (!ctx->scheduler_->has_schedule() && !sanity_check(ctx, ctx->delivered_candidates_))
	{
		ADS_LOG((LP_ERROR, "sanity check failed. transaction_id: %s\n", ctx->transaction_id_.c_str()));
		Ads_Error_Stats::instance()->err(Ads_Error_Stats::ERR_SANITY_CHECK_FAILED);
		Ads_Monitor::stats_inc(Ads_Monitor::SANITY_CHECK_FAILED);
	}
#endif
	if (ctx->scheduler_->is_live_schedule())
	{
		ctx->scheduler_->update_error_code(false/*is_error_exit*/, ctx->delivered_candidates_.empty(), ctx->error_logs_, ctx->errors_);
	}

	// Canoe Service Assurance: PUB-485, PUB-1639, PUB-1640
	if (ctx->request_->is_vod_assurance_needed())
	{
#if defined(ADS_ENABLE_LIBADS)
		Ads_VOD_Service_Assurance::post_request_to_endpoint(ctx->access_paths(), ctx->video_slots_, *ctx->request_, *ctx->response_);
#else
		Ads_VOD_Service_Assurance::post_request_to_endpoint(ctx->access_paths(), ctx->video_slots_, *ctx->request_);
#endif
	}

	ctx->response_->initialize(ctx->request_);

	// [YT] they are filling more delivered candidates: syncing creative and varilable creative
	{
		Ads_GUID_Set video_ads, section_ads;
		//		std::list<Ads_Advertisement_Candidate_Ref *> new_refs;


		if (ctx->response_->rep()->custom_template_ == "youtube")
		{
			ctx->delivered_candidates_.sort(Ads_Advertisement_Candidate_Ref::greater_for_yt());
		}

		bool youtube_slot_selected = false; // FDB-12272, for youtube, Ads should only select one temporal slot


		for (std::list<Ads_Advertisement_Candidate_Ref *>::const_iterator it = ctx->delivered_candidates_.begin();
		        it != ctx->delivered_candidates_.end();
		        ++it)
		{
			Ads_Advertisement_Candidate_Ref *candidate_ref = *it;
			const Ads_Advertisement_Candidate* candidate = candidate_ref->candidate_;
			Ads_Slot_Base *slot = candidate_ref->slot_;

			ADS_ASSERT(slot);
			if (!slot) continue;

			// FDB-12272, keep select companion
			if (youtube_slot_selected && !(candidate_ref->is_companion_ && candidate_ref->leader()))
			{
				continue;
			}

			if (!(slot->flags_ & Ads_Slot_Base::FLAG_TRACKING_ONLY) && !candidate_ref->creative_ && !candidate_ref->no_creative_)
			{
				Ads_GUID driven_id = candidate_ref->is_companion_ && candidate_ref->leader() ? candidate_ref->leader()->candidate_->ad_->id_ : candidate->ad_->id_;
				Ads_Selector::choose_creative_rendition_for_advertisement(ctx, candidate, slot, candidate_ref->creative_, driven_id, candidate->applicable_creatives(slot));

				candidate_ref->no_creative_ = !candidate_ref->creative_;
			}
			else if (candidate->ad_->is_tracking() && ctx->request_->rep()->capabilities_.support_null_creative_)
			{
				const_cast<Ads_Advertisement_Candidate_Ref *>(candidate_ref)->creative_ = &ctx->repository_->null_rendition_;
			}

			ADS_DEBUG((LP_TRACE, "    ad %s creative %s in slot %s[%d]:%s\n",
			           candidate->ad_? CANDIDATE_LITE_ID_CSTR(candidate->lite_) : "",
			           candidate_ref->creative_? CREATIVE_EXTID_CSTR(candidate_ref->creative_): "",
			           ads::env_name(slot->env()),
			           slot->position_,
			           slot->custom_id_.c_str()));

			if (slot->env() == ads::ENV_PAGE) section_ads.insert(candidate->ad_->id_);
			else video_ads.insert(candidate->ad_->id_);

			// display refresh history
			// put last ad at the beginning of the list
			if ((ctx->refresh_config_ || ctx->request_->rep()->capabilities_.refresh_display_on_demand_)
				&& (slot->env() == ads::ENV_PAGE || slot->env() == ads::ENV_PLAYER))
			{
				ctx->user_->update_display_refresh_history(slot->custom_id_, candidate->ad_id());
			}

#if !defined(ADS_ENABLE_FORECAST)
			// server side vast translation
			if (slot->env() == ads::ENV_VIDEO)
			{
				auto mutable_ref = const_cast<Ads_Advertisement_Candidate_Ref*>(candidate_ref);
				ctx->take_external_ad_for_translation(mutable_ref);
			}
#endif

			/// HOOK: ads in video slots already counted
			if (slot->env() != ads::ENV_VIDEO && !candidate_ref->is_fallback_) ++const_cast<Ads_Slot_Base *>(slot)->num_advertisements_;

			/// HOOK: record duration
			/// TODO: remove this?
			if (ctx->response_->format() == Ads_Response::LEGACY
				&& slot->env() == ads::ENV_VIDEO
				&&  ctx->response_->duration() == 0 && candidate_ref->creative_)
			{
				double duration = candidate_ref->creative_->duration_;
				if (slot->impression_weight_ >= 0) duration *= slot->impression_weight_ ;
				ctx->response_->duration(duration);
			}

			/// HOOK: log frequence cap
			if (!ctx->is_prefetch() && ctx->fc_->is_frequency_cap_enabled())
			{
				//no prefetch means ADS will not wait for callback to log frequency cap.
				//Usually,no prefetch request is for display ad,and there is at most one ad in this request.
				ctx->fc_->log_ad_views(*ctx, *candidate);
			}
			if (ctx->response_->rep()->custom_template_ == "youtube")
			{
				youtube_slot_selected = true;
			}
		}

		// Write A/B test information
		if (ctx->response_->store_ab_test_info(ctx->repository_->ab_test_collections_, ctx->ab_test_active_buckets_) != 0)
		{
			ADS_LOG((LP_ERROR, "Error during stroe AB test information\n"));
		}

		bool variable_ad_filled_in_request = false;
		// Fill variable duration ad, including variable ad, padding ad, placeholder ad, honor min_duration of video slot
		for (auto *slot : ctx->video_slots_)
		{
			Ads_Video_Slot *vslot = dynamic_cast<Ads_Video_Slot *>(slot);
			if (slot == nullptr || vslot == nullptr)
			{
				ADS_LOG((LP_ERROR, "NULL slot met[%p/%p]\n", slot, vslot));
				continue;
			}

			bool variable_ad_filled_in_slot = false;
			if (fill_variable_ad_into_slot(ctx, slot, ctx->scheduler_->placeholder_candidate_, ctx->scheduler_->placeholder_creative_id_, variable_ad_filled_in_slot) < 0)
			{
				ADS_LOG((LP_ERROR, "fail to fill variable ad\n"));
			}
			else
			{
				if (variable_ad_filled_in_slot && !variable_ad_filled_in_request)
				{
					variable_ad_filled_in_request = true;
				}
			}

			/// FDB-3827
			if (vslot->duration_remain_ <= ctx->repository_->system_config().min_slot_remaining_duration_ && vslot->max_num_advertisements_ > vslot->num_advertisements_)
			{
				ADS_DEBUG((LP_DEBUG, "change max ad number of slot when duration remain is less than min_slot_remaining_duration_ %d, custom_id %s, duration_remain %d, max_ad_number %d, filled_ad_number %d\n"
							, ctx->repository_->system_config().min_slot_remaining_duration_, vslot->custom_id_.c_str(), vslot->duration_remain_
							, vslot->max_num_advertisements_, vslot->num_advertisements_));
				vslot->apply_max_num_advertisements(vslot->num_advertisements_, false /*set_raw_max_num_ad*/);
			}
		}

		if (variable_ad_filled_in_request)
		{
			this->calculate_advertisement_position_in_slot(ctx, ctx->delivered_candidates_);
		}

#if !defined(ADS_ENABLE_FORECAST)
		/// ad bumpers
		append_bumpers(ctx, ctx->delivered_candidates_);
#endif

		/// FDB-3827
		if (ctx->tracking_slot_)
		{
			ctx->tracking_slot_->apply_max_num_advertisements(ctx->tracking_slot_->num_advertisements_, false /*set_raw_max_num_ad*/);
		}

		this->update_rule_counter_for_wasted_inventory(ctx);
		/// save selection history in state & cookie
		if (ctx->request_->rep()->capabilities_.synchronize_multiple_requests_)
		{
			if (!video_ads.empty() || !section_ads.empty())
			{
				ctx->request_->user_->synchronize_ads_for_multiple_requests(video_ads, section_ads);
			}
		}

		ctx->user_->add_advertisement_delivery_record(Ads_User_Info::REQUEST_SCOPE_VIDEO, ctx->time(), ctx->request_->video_random(), video_ads);
		ctx->user_->add_advertisement_delivery_record(Ads_User_Info::REQUEST_SCOPE_PAGE, ctx->time(), ctx->request_->page_random(), section_ads);
	}


	{
		/// update counters
		if (ctx->should_update_counter())
		{
			bool deferred = (ctx->is_prefetch() && !ctx->is_tracking());
			this->update_rules_and_counters(ctx, deferred);
			if (!deferred)
			{
				this->stats_.log_ack(true, false, false, false);
				this->log_ack(true, false, false, false);
				if (ctx->request_->custom_counter_id() >= 0)
					this->stats_.log_ack((Ads_GUID)ctx->request_->custom_counter_id(), true, false, false, false);
			}
		}

		/// add video view time for non-prefetch requests
		if (!ctx->video_slots_.empty() && ctx->response_->format() == Ads_Response::LEGACY && ctx->response_->duration() > 0)
		{
			if (ctx->section_ && ctx->ux_conf_ && ctx->ux_conf_->commercial_ratio_type_ == Ads_User_Experience_Config::PERCENTAGE)
			{
				ctx->user_->add_commercial_view_ratio_record(ctx->time(),
			        ctx->ux_network_id_,
			        ctx->ux_inventory_id_,
					ctx->ux_conf_->id_,
					0, 0, 0, ctx->response_->duration());
			}
		}

		/// serving ad, update ad counter
		/// TODO: only count when it is the first ad call during the video view
		if (!ctx->is_tracking() && ctx->root_asset_ && ctx->section_ && ctx->ux_conf_)
		{
			if (ctx->ux_conf_->commercial_ratio_type_ == Ads_User_Experience_Config::VIEWS && !ctx->ux_no_video_slots_
				&& !ctx->request_->rep()->capabilities_.skips_ad_selection_)
				ctx->user_->add_commercial_view_ratio_record(ctx->time(),
			        ctx->ux_network_id_,
			        ctx->ux_inventory_id_,
					ctx->ux_conf_->id_,
			        0, 1, 0.0, 0.0);

			if (ctx->request_->rep()->capabilities_.log_video_view_ > 0)
			{
				ctx->user_->add_commercial_view_ratio_record(ctx->time(),
			        ctx->ux_network_id_,
			        ctx->ux_inventory_id_,
					ctx->ux_conf_->id_,
			        1, 0, ctx->request_duration(), 0);
			}
		}
	}

#if !defined(ADS_ENABLE_FORECAST)
	if (ctx->collect_pg_auctions() >= 0)
	{
		ctx->flags_ |= Ads_Selection_Context::FLAG_NEEDS_EXTERNAL_CREATIVES;
	}

	if (ctx->listeners_)
		this->notify_selection_listeners(ctx, Ads_Selection_Listener::STAGE_10);

	ctx->stage_ = 10;

	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_10_0);
	//escape for external ad loading
	if (ctx->needs_external_creatives())
	{
		ALOG(LP_DEBUG, "External ads/creatives loading is needed and jump out of ad selection\n");
		try_reschedule_selection(ctx);
		return 1;
	}

	// Sanity check
	SANITY_TEST(Sanity_Test::TEST_DEBUG, ctx);

__STAGE_11:
	ctx->apm_.start(Ads_Selection_Context::APM::EXTERNAL_IO_AD_CCH_TIME);
	this->log_external_transaction(ctx);
	ctx->add_proxied_cookies();

	ctx->jitt().complete_ingestions(ctx);
	ctx->start_market_ad_ingestions();
	ctx->update_global_approval_pending_market_ads();
	ctx->apm_.end(Ads_Selection_Context::APM::EXTERNAL_IO_AD_CCH_TIME);
	ctx->time_external_ad_cch_time_ms_ = ctx->apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_EXTERNAL_IO, Ads_Selection_Context::APM::EXTERNAL_IO_AD_CCH_TIME) * 1.0 / 1000;

	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_11_0);
	for (auto * ref : ctx->delivered_candidates_)
	{
		if (!ref->is_valid())
			continue;
		// OpenRTB
		if (ref->external_ad_info().is_sfx_4_5_ad() || ref->external_ad_info().is_general_dsp_ad())
		{
			ADS_DEBUG((LP_DEBUG, "process openrtb macro for ad: %s\n", CANDIDATE_LITE_LPID_CSTR(ref->candidate_->lite_)));
			ctx->process_substitution_macros(ref);
		}
		// Phase 8 sstf+Phase 10 sstf
		else if (ref->external_translated_ || ref->candidate_->is_pre_selection_external_translated())
		{
			ADS_DEBUG((LP_DEBUG, "process gdpr macro for sstf ad : %s\n", CANDIDATE_LITE_LPID_CSTR(ref->candidate_->lite_)));
			ctx->process_substitution_macros(ref, &Ads_Macro_Expander::gdpr_macros_);
		}
	}
	if (!(ctx->request_flags() & Ads_Request::Smart_Rep::SKIP_GENERATE_RESPONSE))
	{
		this->generate_response_body(ctx);

		for (auto* ref : ctx->delivered_candidates_)
		{
			if (!ref->is_valid() ||
				!ref->external_ad_info().is_general_dsp_ad() ||
				ref->is_pg_td_ad() ||
				ref->external_ad_info().bid_info().nurl_.empty() ||
				!ref->is_delivered_)
			{
				continue;
			}
			auto *lite = ref->candidate_->lite_;
			// who should send win notice: sstf fallback ad/p8 driven ad/successful sstf refactor ad
			ADS_DEBUG((LP_TRACE, "send win notification for ad: %s, nurl: %s\n",
				CANDIDATE_LITE_LPID_CSTR(lite), ref->external_ad_info().bid_info().nurl_.c_str()));

			bool is_ccpa_enforced = (ctx->data_privacy_->is_ccpa_opt_out_enabled()
				&& !selection::Auctioneer_Base::is_ccpa_exempt_for_buyer_platform(*(ctx->data_privacy_), ref->external_ad_info().bid_info().buyer_platform()));
			bool is_gdpr_approved = ctx->is_gdpr_approved(lite->dsp_network_id());
			Ads_Request_Data_Privacy privacy = ctx->request_data_privacy(lite->operation_owner().network_id(), lite->dsp_network_id(), is_gdpr_approved, is_ccpa_enforced);
			if (lite->get_deal() && lite->get_deal()->mkpl_order_ && lite->get_deal()->mkpl_order_->is_lmd_exchange_order())
				privacy = ctx->request_data_privacy(ctx->data_right_management_->dro_network() != nullptr ? ctx->data_right_management_->dro_network()->id_ : ADS_INVALID_NETWORK_ID,
								ADS_INVALID_NETWORK_ID, is_gdpr_approved, is_ccpa_enforced);

			selection::Auctioneer_Base::send_win_notification(ctx, ref->external_ad_info().bid_info().nurl_, privacy);
		}
		// FDB 12272, for youtube, extract slotImression and add to ad's impression tracking
		if (ctx->response_->rep()->custom_template_ == "youtube")
		{
			Ads_String_Pair_Vector youtube_preroll_slot_callbacks;
			Ads_String_Pair_Vector youtube_overlay_slot_callbacks;
			for (std::vector<Ads_Response::Smart_Rep::Site_Section::Video_Player::Video::Video_Ad_Slot*>::iterator sit = ctx->response_->rep()->site_section_.video_player_.video_.slots_.begin();
					sit != ctx->response_->rep()->site_section_.video_player_.video_.slots_.end(); ++sit)
			{
				Ads_Response::Smart_Rep::Site_Section::Video_Player::Video::Video_Ad_Slot *aslot = *sit;
				Ads_String tpc = aslot->time_position_class_;
				ads::tolower(tpc);
				for (std::list<Ads_Response::Smart_Rep::Event_Callback *>::const_iterator eit = aslot->event_callbacks_.begin(); eit != aslot->event_callbacks_.end();	++eit)
				{
					Ads_Response::Smart_Rep::Event_Callback *ec = *eit;
					if (ec->name_ == CALLBACK_NAME_SLOT_IMPRESSION)
					{
						if (tpc == "overlay")
						{
							youtube_overlay_slot_callbacks.push_back(std::make_pair("slot_impression", ec->url_));
							for (std::list<Ads_String_Pair>::iterator stiter = ec->tracking_urls_.begin(); stiter != ec->tracking_urls_.end(); ++stiter)
							{
								youtube_overlay_slot_callbacks.push_back(*stiter);
							}
						}
						else
						{
							youtube_preroll_slot_callbacks.push_back(std::make_pair("slot_impression", ec->url_));
							for (std::list<Ads_String_Pair>::iterator stiter = ec->tracking_urls_.begin(); stiter != ec->tracking_urls_.end(); ++stiter)
							{
								youtube_preroll_slot_callbacks.push_back(*stiter);
							}
						}
						break;
					}
				}
			}
			bool slot_impr_added = false;
			for (std::vector<Ads_Response::Smart_Rep::Site_Section::Video_Player::Video::Video_Ad_Slot*>::iterator sit = ctx->response_->rep()->site_section_.video_player_.video_.slots_.begin();
					sit != ctx->response_->rep()->site_section_.video_player_.video_.slots_.end(); ++sit)
			{
				Ads_Response::Smart_Rep::Site_Section::Video_Player::Video::Video_Ad_Slot *aslot = *sit;
				Ads_String tpc = aslot->time_position_class_;
				ads::tolower(tpc);
				for (std::deque<Ads_Response::Smart_Rep::Site_Section::Ad_Slot::Ad_Reference *>::iterator it = aslot->ads_.begin(); it != aslot->ads_.end(); ++it)
				{
					Ads_Response::Smart_Rep::Site_Section::Ad_Slot::Ad_Reference *ad_ref = *it;
					for (std::list<Ads_Response::Smart_Rep::Event_Callback *>::const_iterator ceit = ad_ref->event_callbacks_.begin(); ceit != ad_ref->event_callbacks_.end(); ++ceit)
					{
						Ads_Response::Smart_Rep::Event_Callback *ec = *ceit;
						if (ec->type_ != "IMPRESSION" || ec->name_ != CALLBACK_NAME_DEFAULT_IMPRESSION)
							continue;
						slot_impr_added = true;
						if (!youtube_preroll_slot_callbacks.empty())
						{
							for (Ads_String_Pair_Vector::iterator stiter = youtube_preroll_slot_callbacks.begin(); stiter != youtube_preroll_slot_callbacks.end(); ++stiter)
							{
								ec->tracking_urls_.push_back(*stiter);
							}
						}
						if (!(tpc == "preroll" && !ad_ref->candidate_ref_->candidate_->ad_->is_external_) && !youtube_overlay_slot_callbacks.empty())
						{
							for (Ads_String_Pair_Vector::iterator stiter = youtube_overlay_slot_callbacks.begin(); stiter != youtube_overlay_slot_callbacks.end(); ++stiter)
							{
								ec->tracking_urls_.push_back(*stiter);
							}
						}
					}
				}
				if (slot_impr_added)
					break;
			}
			if (!slot_impr_added)
			{
				for (std::vector<Ads_Response::Smart_Rep::Site_Section::Ad_Slot*>::iterator sit = ctx->response_->rep()->site_section_.video_player_.slots_.begin();
					sit != ctx->response_->rep()->site_section_.video_player_.slots_.end(); ++sit)
				{
					Ads_Response::Smart_Rep::Site_Section::Ad_Slot *slot = *sit;
					for (std::deque<Ads_Response::Smart_Rep::Site_Section::Ad_Slot::Ad_Reference *>::iterator it = slot->ads_.begin(); it != slot->ads_.end(); ++it)
					{
						Ads_Response::Smart_Rep::Site_Section::Ad_Slot::Ad_Reference *ad_ref = *it;
						for (std::list<Ads_Response::Smart_Rep::Event_Callback *>::const_iterator ceit = ad_ref->event_callbacks_.begin(); ceit != ad_ref->event_callbacks_.end(); ++ceit)
						{
							Ads_Response::Smart_Rep::Event_Callback *ec = *ceit;
							if (ec->type_ != "IMPRESSION" || ec->name_ != CALLBACK_NAME_DEFAULT_IMPRESSION)
								continue;
							slot_impr_added = true;
							if (!youtube_preroll_slot_callbacks.empty())
							{
								for (Ads_String_Pair_Vector::iterator stiter = youtube_preroll_slot_callbacks.begin(); stiter != youtube_preroll_slot_callbacks.end(); ++stiter)
								{
									ec->tracking_urls_.push_back(*stiter);
								}
							}
							if (!youtube_overlay_slot_callbacks.empty())
							{
								for (Ads_String_Pair_Vector::iterator stiter = youtube_overlay_slot_callbacks.begin(); stiter != youtube_overlay_slot_callbacks.end(); ++stiter)
								{
									ec->tracking_urls_.push_back(*stiter);
								}
							}
						}
					}
					if (slot_impr_added)
						break;
				}
			}
			if (!slot_impr_added)
			{
				for (std::vector<Ads_Response::Smart_Rep::Site_Section::Ad_Slot*>::iterator sit = ctx->response_->rep()->site_section_.slots_.begin();
					sit != ctx->response_->rep()->site_section_.slots_.end(); ++sit)
				{
					Ads_Response::Smart_Rep::Site_Section::Ad_Slot *slot = *sit;
					for (std::deque<Ads_Response::Smart_Rep::Site_Section::Ad_Slot::Ad_Reference *>::iterator it = slot->ads_.begin(); it != slot->ads_.end(); ++it)
					{
						Ads_Response::Smart_Rep::Site_Section::Ad_Slot::Ad_Reference *ad_ref = *it;
						for (std::list<Ads_Response::Smart_Rep::Event_Callback *>::const_iterator ceit = ad_ref->event_callbacks_.begin(); ceit != ad_ref->event_callbacks_.end(); ++ceit)
						{
							Ads_Response::Smart_Rep::Event_Callback *ec = *ceit;
							if (ec->type_ != "IMPRESSION" || ec->name_ != CALLBACK_NAME_DEFAULT_IMPRESSION)
								continue;
							slot_impr_added = true;
							if (!youtube_preroll_slot_callbacks.empty())
							{
								for (Ads_String_Pair_Vector::iterator stiter = youtube_preroll_slot_callbacks.begin(); stiter != youtube_preroll_slot_callbacks.end(); ++stiter)
								{
									ec->tracking_urls_.push_back(*stiter);
								}
							}
							if (!youtube_overlay_slot_callbacks.empty())
							{
								for (Ads_String_Pair_Vector::iterator stiter = youtube_overlay_slot_callbacks.begin(); stiter != youtube_overlay_slot_callbacks.end(); ++stiter)
								{
									ec->tracking_urls_.push_back(*stiter);
								}
							}
						}
					}
					if (slot_impr_added)
						break;
				}
			}
		}

		if (ctx->response_->rep()->is_comcast_vod_ws2())
		{
			std::stable_sort(
					ctx->response_->rep()->site_section_.video_player_.video_.slots_.begin(),
					ctx->response_->rep()->site_section_.video_player_.video_.slots_.end(),
					Ads_Response::Smart_Rep::Site_Section::Video_Player::Video::Video_Ad_Slot::Slot_Time_Position_String_Sorter()
				);
		}

		if (ctx->is_tracking() && !ctx->errors_.empty())
		{
			Ads_String e = ctx->response_->rep()->errors_.to_names();
			ctx->response_->add_custom_header("X-FW-Error-Info", e);
		}
	}

		if (ctx->listeners_)
			this->notify_selection_listeners(ctx, Ads_Selection_Listener::STAGE_END);

	if (!(ctx->request_flags() & Ads_Request::Smart_Rep::SKIP_GENERATE_RESPONSE))
	{
		Ads_Response::Smart_Rep *rep = ctx->response_->rep();
		if (rep)
		{
			if (ctx->request_flags() & Ads_Request::Smart_Rep::VERBOSE_LOG_PERFORMANCE)
				rep->flags_ |= Ads_Response::Smart_Rep::FLAG_PERFORMANCE_TRACKING;

			if (ctx->request_->rep() && ctx->request_->rep()->capabilities_.expect_multiple_renditions_)
				rep->flags_ |= Ads_Response::Smart_Rep::FLAG_EXPECT_MULTIPLE_RENDITIONS;

			/// record time elapsed
			ads::Time_Value now = ads::gettimeofday();
			rep->time_elapsed_ = now.msec() - ctx->request_->time_created_real().msec();
			rep->time_asset_lookup_ = ctx->time_asset_lookup_;
			rep->time_userdb_user_lookup_ = ctx->audience_->time_userdb_user_lookup_;
			rep->time_userdb_signal_break_lookup_ = ctx->time_userdb_signal_break_lookup_;

			ctx->time_elapsed_ = rep->time_elapsed_;

			if (ctx->repository_)
			{
				rep->time_repository_ = ctx->repository_->time_started();
				rep->time_asset_repository_ = ctx->repository_->time_started();
			}
			Ads_Error_Stats::instance()->err(Ads_Error_Stats::ERR_TX_SELECTION);
			this->log_latency_metrics(ctx);
		}

		/// build response node
		ctx->response_->try_build_response_node(ctx->need_external_merging(),
		                                        ctx->enable_tracking_url_router_,
		                                        ctx->opt_out_xff_in_tracking_url_router_,
		                                        ctx->request_->proxy_callback_host(),
		                                        ctx->transaction_id_);
	}

	if (ctx->listeners_)
		this->notify_selection_listeners(ctx, Ads_Selection_Listener::STAGE_POST);

	if (ctx->enable_tracer())
		ctx->response_->generate_ui_debug_response();
	
	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_11_0);
	if (ctx->enable_time_logging_)
	{
		if (Ads_Server_Config::instance()->selector_enable_time_logging_for_performance_test_
				|| (ctx->request_flags() & Ads_Request::Smart_Rep::VERBOSE_LOG_TRANSACTION_TIME_SPAN))
		{
			ADS_LOG((LP_INFO, "ADS selection[network:%s profile:%s] time span detail: %s\n", ADS_ENTITY_ID_CSTR(ctx->network_id_),
						ctx->profile_ == nullptr ? "" : ctx->profile_->name_.c_str(), ctx->apm_.to_string().c_str()));
		}
	}

#endif

#if defined(ADS_ENABLE_FORECAST)
__STAGE_11:
__LOG_REQUEST:
	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_10_0);
	//Code go to __LOG_REQUEST b/c of flag SKIP_GENERATE_RESPONSE. However, AF needs to log rule info into bin-log for all 4 scenarios.
	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_11_0);

		for (std::list<Ads_Advertisement_Candidate_Ref *>::iterator it = ctx->delivered_candidates_.begin();
		        it != ctx->delivered_candidates_.end();
		        ++it)
		{
			Ads_Advertisement_Candidate_Ref *candidate_ref = *it;
			const Ads_Advertisement_Candidate* candidate = candidate_ref->candidate_;//non const???
			Ads_Slot_Base *slot = candidate_ref->slot_;
			if (!slot || !candidate->ad_) continue;

			// reuse set replica_id logic in func:generate_response_body
			if (candidate->is_pod_ad())
				candidate_ref->replica_id_ += candidate->lite_->pod_ad_info()->pod_ad_index_;
			if (candidate->bid_replica_id() != size_t(-1))
				candidate_ref->replica_id_ = (candidate->bid_replica_id() << 16) | candidate_ref->replica_id_;

			// pending portfolio resellable
			if (!candidate->ad_->is_portfolio()) continue;

			const Ads_Asset_Section_Closure* closure = candidate->owner_.to_closure();
			if (closure == nullptr) continue;

			if (ctx->root_asset_ && ctx->root_asset_->network_id_ == closure->network_id_)
			{
				if (!ctx->has_committed_path(slot)
						|| ctx->is_committed_through(closure->network_id_, slot)) // for MRM rule loop case
					candidate_ref->flags_ |= Ads_Advertisement_Candidate::FLAG_INVENTORY_COMMITTED;
			}
			else
			{
				if (ctx->is_committed_through(closure->network_id_, slot))
					candidate_ref->flags_ |= Ads_Advertisement_Candidate::FLAG_INVENTORY_COMMITTED;
			}

			if (!candidate->owner_.sellable(slot))
				candidate_ref->flags_ |= Ads_Advertisement_Candidate::FLAG_PORTFOLIO_NON_RESELLABLE;
			ADS_DEBUG((LP_TRACE, "custom portfolio ad %ld for closure %ld slot %s guaranteed: %d, flag:%ld, PORTFOLIO_NON_RESELLABLE: %d\n", ads::entity::id(candidate->ad_->id_), ads::entity::id(closure->network_id_), slot->custom_id_.c_str(), closure->guaranteed(slot), candidate_ref->flags_, candidate_ref->flags_&Ads_Advertisement_Candidate::FLAG_PORTFOLIO_NON_RESELLABLE));
		}
		//request xTR infos
		if (!(ctx->request_flags() & Ads_Request::Smart_Rep::FAST_SIMULATION))
		{
			const Ads_Network* network = 0;
			Ads_GUID_Set::const_iterator it = Ads_Server_Config::instance()->forecast_networks_enabled_xtr_stat_.find(ctx->root_network_id());

			bool need_xtr_stat_pre = false, need_send_xtr_req = false;
			bool xtr_request_fail = false;
			Ads_XTR_Client::xTR_Function_Stat xtr_stat_info;

			if (Ads_Server_Config::instance()->forecast_networks_enabled_xtr_stat_.end() != it)
			{
				need_xtr_stat_pre = true;
			}
			if( Ads_Server_Config::instance()->xtr_config_.enable_xtr_ &&
			    ((ctx->root_asset_ && ctx->repository_->find_network(ctx->root_asset_->network_id_, network) >= 0 && network)
			     || (ctx->root_section_ && ctx->repository_->find_network(ctx->root_section_->network_id_, network) >= 0 && network)) &&
			    ctx->repository_->network_function("FORECAST_ACK_WITH_XTR", network->id_))
			{
				need_send_xtr_req = true;
			}
			if (need_xtr_stat_pre || need_send_xtr_req)
			{
				xtr::xTR_Message xtr_message;
				bool cpx_present = false;
				generate_xtr_message(ctx, xtr_message, xtr_stat_info, cpx_present);
				// DO NOT send req when no_cpx_ad and scenario is not real
				if (!cpx_present && ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::SKIP_GENERATE_RESPONSE)
					need_send_xtr_req = false;

				if (need_send_xtr_req)
				{
					xtr::xTR_Message xtr_resp;
					if (Ads_XTR_Client::instance()->predict(xtr_message, xtr_resp) < 0)
					{
						ADS_LOG((LP_ERROR, "xTR request failed, tid:%s\n", ctx->transaction_id_.c_str()));
						xtr_request_fail = true;
					}
					else
					{
						parse_xtr_message(ctx, xtr_resp);
						ADS_DEBUG((LP_TRACE, "xTR request parse succeed, tid:%s\n", ctx->transaction_id_.c_str()));
					}
				}
				Ads_XTR_Client::instance()->sync_xtr_stat_info(ctx->root_network_id(), xtr_stat_info, xtr_request_fail);

				using ::apache::thrift::to_string;
				ADS_DEBUG((LP_DEBUG, "xTR message: %s\n", to_string(xtr_message).c_str()));
			}
		}
		if (ctx->scheduler_->is_digital_live_schedule() && (ctx->request_flags() & Ads_Request::Smart_Rep::PROPOSAL_IF_NO_BUDGET))
		{
			this->calculate_advertisement_position_in_slot(ctx, ctx->delivered_candidates_);
		}
#endif
		{
			/// logging
			if (Ads_Server_Config::instance()->logger_enable_binary_log_)
			{
			//FIXME: log  KVs from user data
			{
				Ads_String_List& l = ctx->key_values_for_log_;
				std::copy(ctx->request_->rep()->key_values_.pairs_.begin(),
				          ctx->request_->rep()->key_values_.pairs_.end(),
				          std::back_inserter(l));
				if (l.size() > Ads_String_List::size_type(ctx->repository_->system_config().max_logged_request_key_value_num_))
				{
					l.resize(ctx->repository_->system_config().max_logged_request_key_value_num_);
				}
				std::copy(ctx->audience_->cro_logged_user_key_values_.begin(),
				          ctx->audience_->cro_logged_user_key_values_.end(),
				          std::back_inserter(l));
			}


			if ((ctx->root_asset_ || ctx->shadow_asset_)
				&& (ctx->request_->rep()->capabilities_.log_video_view_ > 0)
				&& !ctx->is_tracking())
				ctx->flags_ |= Ads_Selection_Context::FLAG_LOG_VIDEO_VIEW;

			this->log_request(ctx);
		}
#if defined(ADS_ENABLE_FORECAST)
		else
		{
			//FIXME: log  KVs from user data
			if (ctx->is_forecast_nightly())
			{
				Ads_String_List& l = ctx->key_values_for_log_;
				std::copy(ctx->request_->rep()->key_values_.pairs_.begin(),
				          ctx->request_->rep()->key_values_.pairs_.end(),
				          std::back_inserter(l));
				if (l.size() > Ads_String_List::size_type(ctx->repository_->system_config().max_logged_request_key_value_num_))
				{
					l.resize(ctx->repository_->system_config().max_logged_request_key_value_num_);
				}
				std::copy(ctx->audience_->cro_logged_user_key_values_.begin(),
				          ctx->audience_->cro_logged_user_key_values_.end(),
				          std::back_inserter(l));
			}

			//In UGA/GA/NA scenarios, rule info is not updated into bin log. so we need to update rulel info into bin log here.
			if(ctx->should_update_counter() && (ctx->request_flags() & Ads_Request::Smart_Rep::SKIP_GENERATE_RESPONSE))
			{
				bool deferred = (ctx->is_prefetch() && !ctx->is_tracking());
				this->update_rules_and_counters(ctx, deferred);
			}
			this->log_request(ctx, false);
		}
#endif

		if (Ads_Server_Config::instance()->logger_enable_access_log_)
		{
			this->log_transaction(ctx);
		}
	}
#if defined(ADS_ENABLE_FORECAST)
	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_11_0);

	if (Ads_Server_Config::instance()->selector_enable_time_logging_for_performance_test_)
	{
		ADS_LOG((LP_INFO, "ADS selection[network:%s profile:%s xid:%s] time span detail: %s\n", ADS_ENTITY_ID_CSTR(ctx->network_id_),
				ctx->profile_ == nullptr ? "" : ctx->profile_->name_.c_str(), ctx->transaction_id_.c_str(), ctx->apm_.to_string().c_str()));
	}
#endif


	ctx->check_perf_counters();

	this->stats_.log_selection(ctx);
	this->log_selection(ctx);

	log_reject_reason(*ctx);

	if (Ads_Server_Config::instance()->enable_performance_debug_)
	{
		log_performance_info(*ctx);
	}
	if (!ctx->restricted_candidates_logs_.empty())
	{
		json11::Object restriction_log;
		generate_candidate_restriction_log(ctx, restriction_log);
		Ads_Server::elk_log("candidate_restriction_tracking", restriction_log);
	}

	// output mkpl execution troubleshooting log
	ctx->mkpl_executor_->output_execution_loggings(ctx);

	output_ad_candidate_error_log(ctx);

	return 0;

error_exit:
	// keep the old logic, be compatible with the regression case.
	ctx->slot_controller_.initialize(*ctx->repository_, *ctx->request_->rep(), ctx->errors_);
	if (ctx->page_slots_.empty() && ctx->player_slots_.empty())
		ctx->slot_controller_.generate_display_slots(ctx->disable_page_slot_delivery_, ctx->page_slots_, ctx->player_slots_, ctx->dot_slot_);
	if (ctx->video_slots_.empty())
		ctx->slot_controller_.generate_video_slots_by_request_rep(ctx->video_slots_, nullptr);

	++this->stats_.n_selection_failed_;
	Ads_Monitor::stats_inc(Ads_Monitor::TX_SELECTION_FAILED);

	{
		ctx->response_->initialize(ctx->request_);
		
		if (ctx->scheduler_->is_live_schedule())
		{
			ctx->scheduler_->update_error_code(true/*is_error_exit*/, ctx->delivered_candidates_.empty(), ctx->error_logs_, ctx->errors_);
			ctx->scheduler_->initialize_response_info(ctx->response_->rep()->linear_info_, ctx->response_->rep()->replacement_transparency_);
		}

		Ads_Response::Smart_Rep::Errors::build(ctx, ctx->response_->rep()->errors_);

		if (ctx->is_tracking() && !ctx->errors_.empty())
		{
			Ads_String e = ctx->response_->rep()->errors_.to_names();
			ctx->response_->add_custom_header("X-FW-Error-Info", e);
		}
	}

	if (Ads_Server_Config::instance()->logger_enable_binary_log_)
	{
		if (!ctx->scheduler_->is_linear_live_schedule())
		{
			ctx->request_->audit_checker_.set_filtration_reason(Ads_Audit::FILTERED_BY_INTERNAL_ERROR, false);
		}
		this->log_request(ctx);
	}

	if (ctx->listeners_)
		this->notify_selection_listeners(ctx, Ads_Selection_Listener::STAGE_END);

	if (ctx->enable_tracer())
	{
		json::ObjectP r;
		json::ObjectP o;
		ctx->to_ui_debug_json(o);
		r["ui_debug"] = o;

		std::stringstream s;
		json::Writer::Write(r, s);
		ctx->response_->diagnostic(s.str());
		ADS_DEBUG((LP_INFO, "Diagnostic Info:\n%s\n", s.str().c_str()));
	}

	ctx->response_->try_build_response_node(ctx->need_external_merging(),
	                                        ctx->enable_tracking_url_router_,
	                                        ctx->opt_out_xff_in_tracking_url_router_,
	                                        ctx->request_->proxy_callback_host(),
	                                        ctx->transaction_id_);

	if (ctx->listeners_)
		this->notify_selection_listeners(ctx, Ads_Selection_Listener::STAGE_POST);

	if (ctx->enable_tracer())
		ctx->response_->generate_ui_debug_response();

	if(ctx->request_->rep()->is_creative_preview_request_)
	{
		char err[MAX_ERROR_DESC_LEN];
		::snprintf(err, sizeof err, "failed to serve request %s", ctx->request_->uri().c_str());

		ctx->request_->append_error_log(method_name, ELK_ERROR_TYPE_FAIL_TO_SERVE_REQUEST, err);
	}

	return -1;
}

size_t Ads_Selector::collect_network_terms_for_cbp_targeting(const Ads_Selection_Context *ctx, const Ads_Network *network, Ads_GUID_Set &terms) const
{
	if (!network || !network->config()->enable_cbp_advanced_targeting_)
		return 0;

	// collect existing terms
	auto *closure = ctx->closure(network->id_);
	if (closure)
	{
		terms.insert(closure->terms_.begin(), closure->terms_.end());
	}

	// collect global terms
	dispatch_global_terms(*ctx, network->id_, Ads_Term::ALL_TYPE, terms);

	// collect sites
	if (network->config()->enable_cbp_advanced_targeting_on_site_)
	{
		Ads_Asset_Section_Closure_Map closures;
		construct_asset_section_closures(ctx, ctx->root_section_, ads::MEDIA_SITE_SECTION, closures);

		auto it_closure = closures.find(network->id_);
		if (it_closure != closures.end() && it_closure->second != nullptr)
		{
			terms.insert(it_closure->second->terms_.begin(), it_closure->second->terms_.end());
		}

		// release closures
		ads::for_each2nd(closures.begin(), closures.end(), std::mem_fun(&Ads_Asset_Section_Closure::destroy));
	}
	return terms.size();
}

void Ads_Selector::dispatch_global_terms(const Ads_Selection_Context &ctx, Ads_GUID network_id,
                                         int demands, Ads_GUID_Set &closure_terms) const
{
	bool enable_mkpl_for_downstream = ctx.stbvod_mkpl_enabled_programmers_.find(ads::entity::restore_looped_id(network_id)) != ctx.stbvod_mkpl_enabled_programmers_.end();
	for (auto term_id : ctx.global_terms_)
	{
		int term_type = ads::term::type(term_id);
		if (demands != term_type && demands != Ads_Term::ALL_TYPE)
			continue;

		if (!ctx.is_cro(network_id))
		{
		    if (Ads_Term::is_standard_audience_term(term_id) || term_type == Ads_Term::ENDPOINT_OWNER || term_type == Ads_Term::ENDPOINT )
			    continue;
		    if (!enable_mkpl_for_downstream && Standard_Attributes::is_term_of_content_standard_attribute(term_id))
			    continue;
		}

		if (Ads_Term::is_geo_term(term_id))
		{
			if (ctx.data_privacy_->is_geo_targeting_disabled_by_level(term_id))
			{
				continue;
			}
			if ((Ads_Term::TYPE) ads::term::type(term_id) == Ads_Term::ZONE &&
			    !ctx.repository_->is_zone_term_visiable(term_id, network_id) && !ctx.scheduler_->is_linear_live_schedule())
			{
				ADS_DEBUG((LP_DEBUG, "zone item %s is invisible to network %s.\n", ADS_ENTITY_ID_CSTR(term_id), ADS_ENTITY_ID_CSTR(network_id)));
				continue;
			}
		}
		else if ((Ads_Term::TYPE) ads::term::type(term_id) == Ads_Term::AUDIENCE_ITEM)
		{
			//Audience_items from request will be considered following
			continue;
		}
		else if ((Ads_Term::TYPE)term_type == Ads_Term::USER_AGENT)
		{
			const auto platform_info_map_it = ctx.repository_->ua_platform_group_info_map_.find(term_id);
			if (platform_info_map_it == ctx.repository_->ua_platform_group_info_map_.end())
			{
				ADS_LOG((LP_ERROR, "UA term %s not in DB.\n", ADS_ENTITY_ID_CSTR(term_id)));
				continue;
			}
			const auto *ua_platform_info = platform_info_map_it->second;
			if (ua_platform_info == nullptr || (ua_platform_info->network_id_ != network_id && ads::entity::is_valid_id(ua_platform_info->network_id_)))
			{
				continue;
			}
			// add global UA and current network specific UA package in current closure
		}
		closure_terms.emplace(term_id);
	}
}

// PUB-11712, improve diagnostic info in phase 9
// https://wiki.freewheel.tv/display/viqa/Adserving+Tech+Design+Review%3APUB-11712+Improvement+of+the+diagnostic+info+message
void
Ads_Selector::improve_final_candidate_diagnostic_info(const Ads_GUID watched_ad, Ads_Selection_Context& ctx)
{
	// Only log below messages
	static const ads::hash_set<Ads_String> watched_msgs = {"MAX_NUM_ADVERTISEMENTS", "MAX_SLOT_DURATION", "SLOT_REMOVED", "POD_OCCUPIED"};

	auto it_final_candidate = find_if(ctx.all_final_candidates_.begin(),
	                                  ctx.all_final_candidates_.end(),
	                                  [&](Ads_Advertisement_Candidate* candidate)
	                                  {
		                                return candidate->ad_id() == watched_ad;
	                                  });
	if (it_final_candidate == ctx.all_final_candidates_.end())
	{
		// watched_ad already be removed
		return;
	}
	auto it_delivered_candidate = find_if(ctx.delivered_candidates_.begin(),
	                                      ctx.delivered_candidates_.end(),
	                                      [&](Ads_Advertisement_Candidate_Ref* candidate_ref)
	                                      {
		                                    return candidate_ref->ad_id() == watched_ad;
	                                      });
	if (it_delivered_candidate != ctx.delivered_candidates_.end())
	{
		// watched_ad can be delivered, will remove FREQUENCY_CAP msg of watch ad for ESC-28067
		if (ctx.watches_.find(watched_ad) != ctx.watches_.end())
		{
			Ads_Selection_Context::Watch_Info& watch_info = ctx.watches_[watched_ad];
			for (auto it = watch_info.msgs_.begin(); it != watch_info.msgs_.end();)
			{
				if (*it == "FREQUENCY_CAP")
				{
					it = watch_info.msgs_.erase(it);
					continue;
				}
				++it;
			}
		}
		return;
	}
	// watched_ad cannot pass phase 9
	if (ctx.watches_.find(watched_ad) != ctx.watches_.end())
	{
		Ads_Selection_Context::Watch_Info& watch_info = ctx.watches_[watched_ad];
		if (watch_info.msgs_.size() > 0) return; // there is watch info already
		for (const auto& watched_slot_pair : watch_info.slots_)
		{
			for (const Ads_String& msg : watched_slot_pair.second.msgs_)
			{
				for (const Ads_String& s : watched_msgs)
				{
					if (ads::is_prefix(msg, s))
						watch_info.msgs_.insert(s);
				}
			}
		}
	}
}

bool
Ads_Selector::try_reschedule_selection(Ads_Selection_Context *ctx, bool detached) const
{
#if defined(ADS_ENABLE_FORECAST) && !defined(ADS_ENABLE_LIBADS)
	return false;
#endif
	ctx->active_ = false;
	ctx->request_->detached(true);
	ctx->response_->detached(true);

	if (!ctx->in_slow_selection_)
		ctx->response_->arg(ctx);

	if (detached)
		ctx->flags_ |= Ads_Selection_Context::FLAG_DETACHED;
	return true;
}

bool Ads_Selector::try_reschedule_selection_external_info_lookup(
	Ads_Selection_Context *ctx,
	const char *tag,
	int timeout_ms,
	int *total_lookup_ms,
	std::vector<selection::External_Info_Loader*> &&loaders) const
{
	if (ctx == nullptr || tag == nullptr)
		return false;

#if defined(ADS_ENABLE_FORECAST)
	if (ctx->is_forecast_od() || ctx->scheduler_->is_ip_linear())
		return false;
#endif

	ctx->initialize_external_info_loaders(tag, timeout_ms, total_lookup_ms);
	for (auto &loader : loaders)
	{
		if (loader != nullptr)
			ctx->add_external_info_loader(loader);
	}
	if (ctx->needs_external_info())
	{
		if (!try_reschedule_selection(ctx))
		{
			ADS_LOG((LP_ERROR, "%s: failed to reschedule selection\n", tag));
		}
		else
		{
#if defined(ADS_ENABLE_LIBADS)
			ctx->count_tx_data_lookup_time(total_lookup_ms);
#endif
			ADS_DEBUG((LP_DEBUG, "%s: rescheduling selection\n", tag));
			return true;
		}
	}
	else
	{
		ADS_DEBUG((LP_DEBUG, "%s: no need to reschedule selection\n", tag));
	}
	ctx->clear_external_info_loaders();

	return false;
}

#if defined(ADS_ENABLE_LIBADS)
bool Ads_Selector::reschedule(Ads_Selection_Context *ctx, const char *tag) const
{
	ADS_DEBUG((LP_DEBUG, "reschedule %s\n", tag));
	if (!ctx->has_data_tasks())
		return false;

	ctx->active_ = false;
	ctx->request_->detached(true);
	ctx->response_->detached(true);

	if (!ctx->in_slow_selection_)
		ctx->response_->arg(ctx);

	ctx->flags_ |= Ads_Selection_Context::FLAG_RESTART;

	ctx->count_tx_data_lookup_time(&ctx->time_reschedule_map_[tag]);
	return true;
}
#endif

bool Ads_Selector::try_reschedule_io_tasks(Ads_Selection_Context *ctx, int timeout_ms, int *lookup_time) const
{
#if defined(ADS_ENABLE_LIBADS)
	if (ctx->has_data_tasks())
	{
		ctx->set_external_io_timeout(timeout_ms);
		if (try_reschedule_selection(ctx))
		{
			ctx->count_tx_data_lookup_time(lookup_time);
			return true;
		}
	}
#endif
	return false;
}

int
Ads_Selector::initialize_selection_context(Ads_Selection_Context *ctx)
{
	ADS_ASSERT(ctx && ctx->request_ && ctx->response_ && ctx->repository_ && ctx->user_);

	const Ads_Repository *repo = ctx->repository_;
	Ads_Request *req = ctx->request_;

	//ESC-28577:We need to monitor GET request query string length, if the length is more than 20k, clientIP in header maybe truncated. Unexpected clientIP affect ivt service.
	if (req->method_ == Ads_Request::GET)
	{
		ctx->decision_info_.set_values(Decision_Info::INDEX_13, req->uri().length());
	}

	/// delta repository, move ahead for AF delta repo find break
	if (Ads_Server_Config::instance()->enable_delta_repository_ && ctx->delta_repository_ == nullptr)
	{
		if (!req->rep()->repository_tag_.empty())
		{
			Ads_Delta_Repository *repo = 0;
			if (Ads_Cacher::instance()->get_delta_repository(req->rep()->repository_tag_, repo) >= 0)
			{
				ctx->delta_repository_ = repo;
				ctx->own_delta_repository_ = false;
			}
		}
		else if (!req->o().reload_ad_.empty())
		{
			int flags = 0;
			Ads_Delta_Repository *delta_repo = new (std::nothrow) Ads_Delta_Repository;
			if (delta_repo == nullptr)
			{
				ADS_LOG((LP_ERROR, "operator new Ads_Delta_Repository failed.\n"));
				return -1;
			}

			Ads_GUID_Set ids;
			ads::str_to_entities(req->o().reload_ad_, ADS_ENTITY_TYPE_ADVERTISEMENT, 0, std::inserter(ids, ids.begin()));

			Ads_String_List dimensions;
			if (Ads_Delta_Repo_Loader::instance()->load_delta_repository(repo, Ads_Delta_Repo_Loader::FLAG_DELTA_LOAD_PLACEMENT, flags, ids, dimensions, *delta_repo) >= 0)
			{
				ctx->delta_repository_ = delta_repo;
				ctx->own_delta_repository_ = true;
			}
			else
			{
				// release delta repository
				ADS_LOG((LP_ERROR, "failed to load placement delta repository.\n"));
				delta_repo->destroy();
				delta_repo = nullptr;
			}
		}
	}

	/// distributor
	ctx->network_id_ = req->network_id();

	const Ads_Network *network = 0;
	if (!ads::entity::is_valid_id(ctx->network_id_)
	        || repo->find_network(ctx->network_id_, network) < 0
	        || !network)
	{
		ctx->add_error(Error::GENERAL, Error::NETWORK_NOT_FOUND);
		ctx->error(
		    ADS_ERROR_NETWORK_NOT_FOUND,
		    ads::Error_Info::SEVERITY_NORMAL,
		    ADS_ERROR_NETWORK_NOT_FOUND_S,
		    "",
		    ADS_ENTITY_ID_STR(ctx->network_id_));

		ADS_ERROR_RETURN((LP_ERROR, "no network specified in request.\n"), -1);
	}
	ctx->network_ = network;
	/// asset network
	ctx->asset_network_ = 0;
	Ads_GUID video_asset_network_id = ads::entity::invalid_id(ADS_ENTITY_TYPE_NETWORK);
	if (req->rep()->site_section_.video_player_.video_.valid_)
	{
		const Ads_Network *asset_network = 0;
		const Ads_String & s = req->rep()->site_section_.video_player_.video_.network_id_;
		if (!s.empty())
		{
			video_asset_network_id = ads::entity::make_id(ADS_ENTITY_TYPE_NETWORK, ads::str_to_i64(s));
			if (repo->find_network(video_asset_network_id, asset_network) < 0 || !asset_network)
			{
				if(!(ctx->error_flags_ & Ads_Selection_Context::ERROR_ASSET_NETWORK_NOT_FOUND))
				{
					ctx->add_error(Error::GENERAL, Error::NETWORK_NOT_FOUND);
					ctx->error(
							ADS_ERROR_NETWORK_NOT_FOUND,
							ads::Error_Info::SEVERITY_WARN,
							ADS_ERROR_NETWORK_NOT_FOUND_S,
							"",
							s);
					ctx->error_flags_ |= Ads_Selection_Context::ERROR_ASSET_NETWORK_NOT_FOUND;
				}
			}
		}
		else
		{
			asset_network = ctx->network_;
		}

		ctx->asset_network_ = asset_network;
	}

	/// site section network
	ctx->section_network_ = 0;
	if (req->rep()->site_section_.valid_)
	{
		const Ads_Network *section_network = 0;

		const Ads_String& s = req->rep()->site_section_.network_id_;
		if (!s.empty())
		{
			Ads_GUID network_id = ads::entity::make_id(ADS_ENTITY_TYPE_NETWORK, ads::str_to_i64(s));
			if (repo->find_network(network_id, section_network) < 0 || !section_network)
			{
				if(!(ctx->error_flags_ & Ads_Selection_Context::ERROR_SECTION_NETWORK_NOT_FOUND))
				{
					ctx->error(
							ADS_ERROR_NETWORK_NOT_FOUND,
							ads::Error_Info::SEVERITY_WARN,
							ADS_ERROR_NETWORK_NOT_FOUND_S,
							"",
							s);
					ctx->error_flags_ |= Ads_Selection_Context::ERROR_SECTION_NETWORK_NOT_FOUND;
				}
			}
		}
		else
		{
			section_network = ctx->network_;
		}

		ctx->section_network_ = section_network;
	}

	// Set ctx->enable_site_section_deep_lookup_ default value. INFR-275-SS.
	ctx->enable_site_section_deep_lookup_ = false;

	// If Site Section FOD is closed, only TF1 site sections can access KV Cache. INFR-275-SS.
	if (Ads_Server_Config::instance()->pusher_load_flags_ & Ads_Server_Config::LOAD_INACTIVE_SITE_SECTIONS)
	{
		if (ctx->section_network_ != nullptr)
		{
			if (ads::entity::id(ctx->section_network_->id_) == 506334 || ads::entity::id(ctx->section_network_->id_) == 506335)
			{
				ctx->enable_site_section_deep_lookup_ = true;
			}
		}
	}
	else
	{
		ctx->enable_site_section_deep_lookup_ = true;
	}

	const Ads_Asset_Base *asset = nullptr;
	const Ads_Asset_Base *section = nullptr;
	const Ads_Asset_Base *dummy_asset = nullptr;

	//lookup only in REPA
	bool asset_found  = ctx->find_asset(req->custom_asset_id_, req->asset_id_, asset);
	bool section_found = ctx->find_section(req->custom_section_id_, req->section_id_, section);
	bool associated_asset_found = true; // Not need load associate asset by default.

	if (ctx->is_ad_framework_integration())
	{
		this->parse_profile_in_advance_for_flex_integration(*ctx);
		associated_asset_found = ctx->find_asset(ctx->associated_asset_fod_custom_id(), ctx->associated_asset_id_, dummy_asset);
		ADS_DEBUG((LP_DEBUG, "ctx->associated_asset_fod_custom_id() is %s, ctx->associated_asset_id_ is %s.\n",
			ctx->associated_asset_fod_custom_id().c_str(), ADS_ENTITY_ID_CSTR(ctx->associated_asset_id_)));
	}

	if (Ads_Server_Config::instance()->enable_debug_)
	{
		ALOG(LP_DEBUG, "async_fod_first_time_ = %s, asset_found = %s, section_found = %s\n",
					ctx->async_fod_first_time_?"true":"false",  asset_found?"true":"false", section_found?"true":"false");
		ALOG(LP_DEBUG, "ctx->assets  is %s, asset = %p, asset_custom_id_ = %s, asset_id_ = %s\n",
					ctx->assets_?"not null":"null",  asset, req->custom_asset_id_.c_str(), ADS_ENTITY_ID_CSTR(req->asset_id_));
		ALOG(LP_DEBUG, "ctx->site_sections is %s, site_section = %p, custom_section_id_ = %s , section_id_= %s\n",
					ctx->site_sections_?"not null":"null",  section, req->custom_section_id_.c_str(), ADS_ENTITY_ID_CSTR(req->section_id_));
		ALOG(LP_DEBUG, "req->fallback_asset_id = %s, req->fallback_section_id = %s \n",
					ADS_ENTITY_ID_CSTR(req->fallback_asset_id()), ADS_ENTITY_ID_CSTR(req->fallback_section_id()));
		for (const auto& fallback_custom_section_id : req->fallback_site_section_custom_id_with_network_list_)
			ALOG(LP_DEBUG, "req->fallback_section_custom_id = %s \n", fallback_custom_section_id.c_str());
	}

	if (asset_found)
		ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_26); // asset found in repa

	if ( !asset_found ||!section_found || !associated_asset_found)
	{
		if ((Ads_Server_Config::instance()->pusher_load_flags_ & Ads_Server_Config::ENABLE_ASYNC_FOD)  &&
			Ads_Server_Config::instance()->selector_enable_deep_lookup_)
		{
			if (ctx->async_fod_first_time_)
			{
				ctx->async_fod_first_time_ = false;
				ctx->flags_ |= Ads_Selection_Context::FLAG_NEEDS_CACHE_LOOKUP;
				if (!asset_found || !associated_asset_found)
				{
					ctx->apm_.start(Ads_Selection_Context::APM::INTERNAL_IO_ASSET_LOOKUP);
					ctx->flags_ |= Ads_Selection_Context::FLAG_NEEDS_ASSET_LOOKUP;
				}
				if (!section_found)
				{
					ctx->apm_.start(Ads_Selection_Context::APM::INTERNAL_IO_SECTION_LOOKUP);
					ctx->flags_ |= Ads_Selection_Context::FLAG_NEEDS_SECTION_LOOKUP;
				}
				if (this->try_reschedule_selection(ctx))
					return 1;
			}else
			{
				if (ctx->needs_asset_lookup())
				{
					ctx->time_asset_lookup_ = ctx->ticks();
					ctx->apm_.end(Ads_Selection_Context::APM::INTERNAL_IO_ASSET_LOOKUP);

					ctx->flags_ &= ~Ads_Selection_Context::FLAG_NEEDS_ASSET_LOOKUP;
					if (ctx->asset_fod_receiver_->found_by_fallback_)
						ctx->set_invalid_asset_custom_id(req->custom_asset_id_, req->asset_id());

					if (ctx->asset_fod_receiver_->found())
						ctx->assets_ = ctx->asset_fod_receiver_->entity_;

					if (ads::entity::is_valid_id(ctx->asset_fod_receiver_->id_))
						req->asset_id(ctx->asset_fod_receiver_->id_);

					if (ctx->mkpl_associate_asset_fod_receiver_->found())
					{
						auto mkpl_asset_ptr = ctx->mkpl_associate_asset_fod_receiver_->entity_;
						mkpl_asset_ptr->owner_ = false;
						ctx->assets_->insert(mkpl_asset_ptr->begin(), mkpl_asset_ptr->end());
						ctx->set_associated_asset_id(ctx->mkpl_associate_asset_fod_receiver_->id_);
					}

					if (ctx->asset_fod_receiver_->found_in_repa_)
						ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_26); // asset found in repa

					if (ctx->asset_fod_receiver_->found_in_cache_)
						ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_27); // asset found in cache

					if (ctx->asset_fod_receiver_->not_in_db_)
						ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_28); // asset not in DB

					if (ctx->asset_fod_receiver_->fallback_not_in_db_)
						ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_29); // fallback asset not in DB

					if (ctx->asset_fod_receiver_->found_by_fallback_)
						ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_30);

					if (ctx->asset_fod_receiver_->fod_triggered_)
						ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_31); // asset trigger fod request
				}

				if (ctx->needs_section_lookup())
				{
					ctx->time_asset_lookup_ = ctx->ticks();
					ctx->apm_.end(Ads_Selection_Context::APM::INTERNAL_IO_SECTION_LOOKUP);

					ctx->flags_ &= ~Ads_Selection_Context::FLAG_NEEDS_SECTION_LOOKUP;

					if (ctx->section_fod_receiver_->found_by_fallback_)
						ctx->set_invalid_section_custom_id(req->custom_section_id_, req->section_id());

					if (ctx->section_fod_receiver_->found())
						ctx->site_sections_ = ctx->section_fod_receiver_->entity_;

					if (ads::entity::is_valid_id(ctx->section_fod_receiver_->id_))
						req->section_id(ctx->section_fod_receiver_->id_);
				}
			}
		}
	}

	//lookup ctx->assets_ or ctx->sections , then find in REPA
	asset_found  = (ctx->find_asset(req->asset_id_, asset) == 0);
	section_found = (ctx->find_section(req->section_id_, section) == 0);

	//[begin]add for asset alias for
	if (!ctx->is_inital_profile_)
	{
		this->parse_profile(*ctx);
		ctx->is_inital_profile_ = true;
	}
	//[end]add for asset alias for
	if (Ads_Server_Config::instance()->enable_debug_)
	{
		ALOG(LP_DEBUG, "after : async_fod_first_time_ = %s, asset_found = %s, section_found = %s\n",
					ctx->async_fod_first_time_?"true":"false",  asset_found?"true":"false", section_found?"true":"false");
		ALOG(LP_DEBUG, "after : assets is %s, asset = %p, asset_custom_id_ = %s, asset_id_ = %s\n",
					ctx->assets_?"not null":"null",  asset, req->custom_asset_id_.c_str(), ADS_ENTITY_ID_CSTR(req->asset_id_));
		if (asset)
			ALOG(LP_DEBUG, "after : asset->id_ = %s\n", ADS_ENTITY_ID_CSTR(asset->id_));
		ALOG(LP_DEBUG, "after : site_sections is %s, site_section = %p, custom_section_id_ = %s , section_id_= %s\n",
					ctx->site_sections_?"not null":"null",  section, req->custom_section_id_.c_str(), ADS_ENTITY_ID_CSTR(req->section_id_));
		if (section)
			ALOG(LP_DEBUG, "after : section->id_ = %s\n", ADS_ENTITY_ID_CSTR(section->id_));
	}
	
	auto &request_video = req->rep()->site_section_.video_player_.video_;
	
	//Note: Hylda requests may not contain hylda kv, just get hylda custom attributes through inference.
	if (req->is_live_traffic_type(Ads_Request::Live_Traffic_Type::DIGITAL_LIVE) || ctx->scheduler_->is_inference_enabled_by_profile() || req->is_live_linear())
	{
		if (ctx->scheduler_->parse_request(*ctx, *req, video_asset_network_id) < 0)
		{
			ADS_LOG((LP_ERROR, "parse scheduler request error.\n"));
			return -1;
		}
	}
	if (ctx->scheduler_->is_live_schedule())
	{
		int ret = ctx->scheduler_->initialize_info_by_request(*ctx, *repo, ctx->delta_repository_, *req, asset);
		if (ret > 0)
		{
#if defined(ADS_ENABLE_LIBADS) && !defined(ADS_ENABLE_FORECAST)
			ctx->scheduler_->query_airing_break_info(*ctx);
			EXEC_IO("airing_break_lookup");
#endif
		}
		else if (ret < 0)
		{
			ADS_DEBUG((LP_DEBUG, "initialize_info_by_request fail.\n"));
			ctx->scheduler_->set_status(Ads_Scheduler::INITIALIZE_INFO_FAILED);
		}
	}
	

	ctx->asset_id_ = req->asset_id();
	ctx->section_id_ = req->section_id();
	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_0_1);

	// live linear has no asset in request url
	if (!asset && !ctx->scheduler_->is_linear_live_schedule())
	{
		ADS_LOG((LP_ERROR, "asset %s not found, custom_id: %s\n", ADS_ENTITY_ID_CSTR(req->asset_id()), req->custom_asset_id_.c_str()));
	}

	if (asset && !request_video.network_id_.empty() && ctx->asset_network_ && asset->network_id_ != ctx->asset_network_->id_)
	{
		//prevent misuse of fallbackIds
		ADS_DEBUG((LP_ERROR, "asset %s not belong to network %s\n", ADS_ENTITY_ID_CSTR(asset->id_), ADS_ENTITY_ID_CSTR(ctx->asset_network_->id_)));
		asset = 0;
	}

	if (!section)
	{
		ADS_LOG((LP_ERROR, "section %s not found, custom_id: %s\n", ADS_ENTITY_ID_CSTR(ctx->section_id_), req->custom_section_id_.c_str()));
	}

	if (section && !ctx->request_->rep()->site_section_.network_id_.empty() && ctx->section_network_ && section->network_id_ != ctx->section_network_->id_)
	{
		//prevent misuse of fallbackIds
		ADS_DEBUG((LP_ERROR, "section %s not belong to network %s\n", ADS_ENTITY_ID_CSTR(section->id_), ADS_ENTITY_ID_CSTR(ctx->section_network_->id_)));
		section = 0;
	}

	/// fallback to RoN
	if (!asset && request_video.valid_)
	{
		if ((ctx->status_ & STATUS_ASSET_NOT_FOUND) == 0)
		{
			if (!ctx->scheduler_->is_linear_live_schedule()) // linear scenario always fallback to RoN
			{
				this->log_selection_asset_not_found(ctx);
			}
		}
		ctx->status_ |= STATUS_ASSET_NOT_FOUND;

		bool invalid_asset_custom_id = false;
		if (!request_video.custom_id_.empty())
		{
			invalid_asset_custom_id = true;
		}
		else if (!request_video.id_.empty())
		{
			if(!(ctx->error_flags_ & Ads_Selection_Context::ERROR_INVALID_ASSET_ID))
			{
				ctx->error(
						ADS_ERROR_INVALID_ASSET_ID,
						ads::Error_Info::SEVERITY_WARN,
						ADS_ERROR_INVALID_ASSET_ID_S,
						"",
						request_video.id_);
				ctx->error_flags_ |= Ads_Selection_Context::ERROR_INVALID_ASSET_ID;
			}
		}

		const Ads_Network *asset_network = ctx->asset_network_;
		const Ads_Network *section_network = ctx->section_network_;
		int fallback_unknown_asset = (req->rep()->capabilities_.fallback_unknown_asset_ >= 0) ?
		                             req->rep()->capabilities_.fallback_unknown_asset_
		                             : asset_network ? (asset_network->config() != NULL ? asset_network->config()->enable_unknown_asset_ : (short)Ads_Network::UNKNOWN_ASSET_FALLBACK) : 0;
		bool enable_unknown_asset = true;

		if (fallback_unknown_asset  == Ads_Network::UNKNOWN_ASSET_NO_INVENTORY)
			enable_unknown_asset = false;
		else if (fallback_unknown_asset  == Ads_Network::UNKNOWN_ASSET_NO_INVENTORY_FOR_REQUEST)
		{
			enable_unknown_asset = false;

			///HACK: FDB-2083: disable request inventory when asset is not found
			if (asset_network && asset_network == section_network)
			{
				ADS_DEBUG((LP_TRACE, "no inventory for the whole request\n"));
				ctx->disable_page_slot_delivery_ = true;
			}
		}

		if (asset_network)
		{
			if (enable_unknown_asset)
			{
				/// fall back to RON if applicable
				ctx->asset_id_ = asset_network->run_of_network_id(ads::MEDIA_VIDEO);
				ctx->find_asset(ctx->asset_id_, asset);

				if (asset_network->config() && asset_network->config()->enable_unknown_asset_  == Ads_Network::UNKNOWN_ASSET_PROMO_ONLY)
					ctx->status_ |= STATUS_ASSET_PROMO_ONLY;
				ADS_DEBUG((LP_DEBUG, "fallback to RoN asset %s\n", ADS_ASSET_ID_CSTR(ctx->asset_id_)));
				ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_2); // fallback to RON
			}
			else if (invalid_asset_custom_id)
			{
				Ads_GUID asset_id = asset_network->run_of_network_id(ads::MEDIA_VIDEO);
				ctx->find_asset(asset_id, ctx->shadow_asset_);
			}
		}
	}

	/// check asset and report error
	if (ads::entity::is_valid_id(ctx->asset_id_))
	{
		if (asset)
		{
			ctx->asset_ = (const Ads_Asset *)asset;
			if (ctx->network_id_ != asset->network_id_)
			{
				ADS_DEBUG((LP_TRACE,
				           "context network id %s not match asset network id %s\n",
				           ADS_ENTITY_ID_CSTR(ctx->network_id_),
				           ADS_ENTITY_ID_CSTR(asset->network_id_)));
			}
		}
		else
		{
			ADS_DEBUG((LP_ERROR, "asset %s not found\n", ADS_ASSET_ID_CSTR(ctx->asset_id_)));

			ctx->asset_id_ = ads::entity::invalid_id(ADS_ENTITY_TYPE_ASSET);
			ctx->asset_ = 0;

			if(!(ctx->error_flags_ & Ads_Selection_Context::ERROR_ASSET_NOT_FOUND))
			{
				ctx->error(
						ADS_ERROR_ASSET_NOT_FOUND,
						ads::Error_Info::SEVERITY_WARN,
						ADS_ERROR_ASSET_NOT_FOUND_S,
						"",
						ADS_ASSET_ID_CSTR(ctx->asset_id_));
				ctx->error_flags_ |= Ads_Selection_Context::ERROR_ASSET_NOT_FOUND;
			}
		}
	}

	if (!section)
	{
		if (((ctx->status_ & STATUS_SITE_SECTION_NOT_FOUND) == 0) && req->rep()->site_section_.valid_)
		{
			this->log_selection_section_not_found(ctx);
		}
		ctx->status_ |= STATUS_SITE_SECTION_NOT_FOUND;

		bool invalid_section_custom_id = false;
		if (req->rep()->site_section_.valid_ && !req->rep()->site_section_.custom_id_.empty())
		{
			invalid_section_custom_id = true;
		}
		else if (req->rep()->site_section_.valid_ && !req->rep()->site_section_.id_.empty())
		{
			if(!(ctx->error_flags_ & Ads_Selection_Context::ERROR_INVALID_SECTION_ID))
			{
				ctx->error(
						ADS_ERROR_INVALID_SECTION_ID,
						ads::Error_Info::SEVERITY_WARN,
						ADS_ERROR_INVALID_SECTION_ID_S,
						"",
						req->rep()->site_section_.id_);
				ctx->error_flags_ |= Ads_Selection_Context::ERROR_INVALID_SECTION_ID;
			}
		}

		const Ads_Network *asset_network = ctx->asset_network_;
		const Ads_Network *section_network = ctx->section_network_;
		if (section_network)
		{
			const Ads_Network *section_owner = section_network;

			///XXX: temporary solution for "case 7" (network=LWDist & siteSectionNetworkId=NULL & siteSectionCustomId=NULL)
			if (network->type_ == Ads_Network::LIGHT && network == section_network && asset_network)
				section_owner = asset_network;

			/// fall back to RON if applicable
			int fallback_unknown_section = (req->rep()->capabilities_.fallback_unknown_section_ >= 0) ?
		    	                           req->rep()->capabilities_.fallback_unknown_section_
		        	                       : section_owner->config() != NULL ? section_owner->config()->enable_unknown_asset_ : (short) Ads_Network::UNKNOWN_ASSET_FALLBACK;
			bool enable_unknown_section = true;

			if (fallback_unknown_section  == Ads_Network::UNKNOWN_ASSET_NO_INVENTORY
		        || fallback_unknown_section  == Ads_Network::UNKNOWN_ASSET_NO_INVENTORY_FOR_REQUEST)
				enable_unknown_section = false;
			if (enable_unknown_section)
			{
				//FDB-2477
				if (network->type_ == Ads_Network::LIGHT)
				{
					const Ads_Asset_Base *dsection = 0;
					Ads_GUID dsection_id = section_owner->upstream_network_ron_mirror_id(ads::MEDIA_SITE_SECTION, network->id_);
					if (ads::entity::is_valid_id(dsection_id) && ctx->find_section(dsection_id, dsection) >= 0 && dsection)
					{
						section = dsection;
						ctx->section_id_ = dsection_id;
					}
				}

				if (!section)
				{
					ctx->section_id_ = section_network->run_of_network_id(ads::MEDIA_SITE_SECTION);
					ctx->find_section(ctx->section_id_, section);
				}

				if (section_network->config() && section_network->config()->enable_unknown_asset_ == Ads_Network::UNKNOWN_ASSET_PROMO_ONLY)
					ctx->status_ |= STATUS_SITE_SECTION_PROMO_ONLY;

				ADS_DEBUG((LP_DEBUG, "fallback to RoN section %s\n", ADS_ASSET_ID_CSTR(ctx->section_id_)));
				ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_2); // fallback to RON
			}
			else if (invalid_section_custom_id)
			{
				Ads_GUID section_id = section_network->run_of_network_id(ads::MEDIA_SITE_SECTION);
				ctx->find_section(section_id, ctx->shadow_section_);
			}
		}
	}

	/// check site section
	if (ads::entity::is_valid_id(ctx->section_id_))
	{
		if (section)
		{
			ctx->section_ = (Ads_Section *)section;
			if (ctx->network_id_ != section->network_id_)
			{
				ADS_DEBUG((LP_TRACE,
				           "context network id %s not match section network id %s\n",
				           ADS_ENTITY_ID_CSTR(ctx->network_id_),
				           ADS_ENTITY_ID_CSTR(section->network_id_)));
			}
		}
		else
		{
			ADS_DEBUG((LP_ERROR, "section (raw_str:%s, make_id:%lld, raw_id:%lld), not found (INIT_CTX_2)\n",
					ADS_ENTITY_ID_CSTR(ctx->section_id_), ctx->section_id_, ads::entity::id(ctx->section_id_)));

			ctx->section_id_ = ads::entity::invalid_id(ADS_ENTITY_TYPE_SECTION);
			ctx->section_ = 0;
			ctx->status_ |= STATUS_SITE_SECTION_NOT_FOUND;

			if(!(ctx->error_flags_ & Ads_Selection_Context::ERROR_SECTION_NOT_FOUND))
			{
				ctx->error(
						ADS_ERROR_SECTION_NOT_FOUND,
						ads::Error_Info::SEVERITY_WARN,
						ADS_ERROR_SECTION_NOT_FOUND_S,
						"",
						ADS_ENTITY_ID_STR(ctx->section_id_));
				ctx->error_flags_ |= Ads_Selection_Context::ERROR_SECTION_NOT_FOUND;
			}
		}
	}

	// request tracking. FW-30555: Move forward.
	// is_request_tracking_allowed(): is_request_tracking_enabled_: only assigned by the code of tracking.
	// is_request_tracking_allowed(): enable_accesslog_output_kafka_: assigned by configuration (Ads_Server_Config::enable_kafka_producer_config) and dispatch order.
	// is_request_tracking_enabled_by_profile(): is_request_tracking_enabled_by_profile_: modified by Ads_Selector::parse_context_profile, which is called by Ads_Selector::parse_profile, so move this code block after the calling (calling point: about 21 lines before here).
	// is_request_tracking_enabled_by_cookie(): overrides_.flags_ & Smart_Rep::TRACK_REQUEST: set by Ads_Request::handle_cookie, unset by Ads_Request::clear_cookies, which are called by Ads_Request::handle_header, Ads_Request::parse or Ads_Request::parse_cookies, which are before or after this function.
	// is_request_tracking_enabled_by_filter(): request_tracking_filter_ set by dispatch order or SSP selection which is called after this function; ctx->asset_id_ and ctx->section_id_ has already been set before this code block; find_root_asset is only called in is_request_tracking_enabled_by_filter.
	// 3 returns above: 2 try_reschedule_selection and 1 delta_repo.
	if (Ads_Server::instance()->is_request_tracking_allowed() && ( req->is_request_tracking_enabled_by_profile()
	    || req->is_request_tracking_enabled_by_cookie() || Ads_Server::instance()->is_request_tracking_enabled_by_filter(ctx)))
	{
		req->is_request_tracking_enabled_ = true;
		ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_3);
		if (ctx->response_) ctx->response_->enable_store_raw_content(true);
	}

	/// first check
	if (!ctx->asset_ && !ctx->section_)
	{
		ctx->add_error(Error::GENERAL, Error::INVALID_REQUEST);
		ctx->error(
		    ADS_ERROR_INVALID_REQUEST,
		    ads::Error_Info::SEVERITY_NORMAL,
		    ADS_ERROR_INVALID_REQUEST_S,
		    "",
		    "");

		ADS_ERROR_RETURN((LP_ERROR, "no asset or section found in request.\n"), -1);
	}

	/// root asset
	if (!ctx->found_root_asset())
	{
		if (ctx->asset_)
		{
			const Ads_Asset_Base *root_asset = 0;
			if (this->find_asset_section_distribution_content_right_path(ctx, ads::MEDIA_VIDEO, ctx->asset_, root_asset, ctx->dist_path(ads::MEDIA_VIDEO), ctx->dist_mirrors(ads::MEDIA_VIDEO), ctx->content_right_path(ads::MEDIA_VIDEO), ctx->content_right_mirrors(ads::MEDIA_VIDEO), false) < 0)
			{
				ADS_DEBUG((LP_ERROR, "no root asset for %s\n", ADS_ASSET_ID_CSTR(ctx->asset_->id_)));

				if (!(ctx->error_flags_ & Ads_Selection_Context::ERROR_ASSET_NO_ROOT))
				{
					ctx->error(
							ADS_ERROR_ASSET_NO_ROOT,
							ads::Error_Info::SEVERITY_WARN,
							ADS_ERROR_ASSET_NO_ROOT_S,
							"",
							ADS_ASSET_ID_CSTR(ctx->asset_->id_));
					ctx->error_flags_ |= Ads_Selection_Context::ERROR_ASSET_NO_ROOT;
				}

				ctx->root_asset_ = 0;
			}
			else
			{
				ctx->root_asset_ = (Ads_Asset *)root_asset;
				ctx->root_asset_id_ = root_asset->id_;
			}
			ctx->flags_ |= Ads_Selection_Context::FLAG_FOUND_ROOT_ASSET;
		}

		if (ctx->section_)
		{
			ALOG(LP_DEBUG, "Begin to find Root Section Here. ctx->section_->id_ = %s, ctx->section_id_ = %s\n",
						ADS_ASSET_ID_CSTR(ctx->section_->id_), ADS_ASSET_ID_CSTR(ctx->section_id_));
			const Ads_Asset_Base *entrance_section = ctx->section_;
			if (ctx->scheduler_->is_programmer_linear() && ctx->scheduler_->programmer_site_section_ != nullptr)
			{
				//for programmer linear use case, there is no relationship between mvpd ss and programmer ss. However, we need to use programmer ss as root ss and also find its upstream ss.
				//At the same time, we also need to record distributor ss into binary log for programmer linear footprint.
				entrance_section = ctx->scheduler_->programmer_site_section_;
				//add ctx section to dist path
				ctx->dist_path(ads::MEDIA_SITE_SECTION).push_back(ctx->section_->network_id_);
				ctx->dist_mirrors(ads::MEDIA_SITE_SECTION).push_back(ctx->section_->id_);
				ctx->inventory_info_[ctx->section_->network_id_].second = ctx->section_->id_;
			}
			const Ads_Asset_Base *root_section = 0;
			if (this->find_asset_section_distribution_content_right_path(ctx, ads::MEDIA_SITE_SECTION, entrance_section, root_section, ctx->dist_path(ads::MEDIA_SITE_SECTION), ctx->dist_mirrors(ads::MEDIA_SITE_SECTION), ctx->content_right_path(ads::MEDIA_SITE_SECTION), ctx->content_right_mirrors(ads::MEDIA_SITE_SECTION), false) < 0)
			{
				ADS_DEBUG((LP_ERROR, "no root section for %s\n", ADS_ASSET_ID_CSTR(ctx->section_->id_)));

				if (!(ctx->error_flags_ & Ads_Selection_Context::ERROR_SECTION_NO_ROOT))
				{
					ctx->error(
							ADS_ERROR_SECTION_NO_ROOT,
							ads::Error_Info::SEVERITY_WARN,
							ADS_ERROR_SECTION_NO_ROOT_S,
							"",
							ADS_ENTITY_ID_STR(ctx->section_->id_));
					ctx->error_flags_ |= Ads_Selection_Context::ERROR_SECTION_NO_ROOT;
				}

				ctx->root_section_ = 0;
			}
			else
			{
				ctx->root_section_ = (Ads_Section *)root_section;
				ctx->root_section_id_ = root_section->id_;
			}
			if (ctx->scheduler_->is_programmer_linear())
			{
				//Linear programmer only have expected mirrors and path on site section level.
				//Report will output delivery for cro/distributor/co according to advertisement node
				ctx->scheduler_->build_asset_path_mirrors(ctx->inventory_info_, ctx->dist_path(ads::MEDIA_SITE_SECTION), ctx->programmer_linear_asset_dist_path_, ctx->programmer_linear_asset_dist_mirrors_);
				ctx->scheduler_->build_asset_path_mirrors(ctx->inventory_info_, ctx->content_right_path(ads::MEDIA_SITE_SECTION), ctx->programmer_linear_asset_content_right_path_, ctx->programmer_linear_asset_content_right_mirrors_);
			}
			ctx->flags_ |= Ads_Selection_Context::FLAG_FOUND_ROOT_ASSET;
		}
	}

	if (!ctx->is_root_asset_from_ad_router() && ctx->force_ad_router_network_as_video_cro_)
	{
		if (reset_root_asset_to_ad_router_rovn(*repo, *ctx) >= 0)
		{
			ctx->flags_ |= Ads_Selection_Context::FLAG_RESET_VIDEO_CRO_TO_AD_ROUTER;
		}
	}

	if (ctx->scheduler_->is_programmer_schedule())
	{
		ctx->scheduler_->update_root_asset(*repo, ctx->root_asset_id_, ctx->root_asset_);
	}

	if (ctx->data_right_management_->initialize_dro_network(*ctx, *repo) < 0)
	{
		ADS_LOG((LP_ERROR, "initialize data protection: DRO not found\n"));
		// no error return
	}

	ctx->request_->audit_checker_.check_dro_invalid_client_addr(ctx->data_right_management_->dro_network(), ctx->request_->client_addr());

#if defined(ADS_ENABLE_LIBADS)
	if (!ctx->scheduler_->is_linear_live_schedule() && !ctx->request_->is_t6_vod())//vod and linear have no compliance requirement
		ctx->request_->audit_checker_.initialize_digital_decision_check(*repo, ctx->root_network_id(), ctx->network_id_);
#endif

	if (ctx->data_privacy_->initialize_context_data_privacy(*ctx, ctx->scheduler_->airing_id_) < 0)
	{
		ADS_LOG((LP_ERROR, "initialize data privacy fail\n"));
		// no error return
	}

	Ads_GUID cro_network_id = ctx->root_asset_ ? ctx->root_asset_->network_id_ : ads::entity::invalid_id(ADS_ENTITY_TYPE_NETWORK);
	if (ctx->data_right_management_->initialize_context_data_right_management(*ctx, *repo, cro_network_id, ctx->scheduler_->is_programmer_linear()) < 0)
	{
		ADS_LOG((LP_ERROR, "initialize data_right_management_ fail\n"));
		// no error return
	}
	//hack sa data rights for stbvod programmers
	if (!ctx->stbvod_mkpl_enabled_programmers_.empty())
	{
		init_sa_rights_for_stbvod_programmers(ctx->stbvod_mkpl_enabled_programmers_, *ctx->data_right_management_);
	}	
	//hack sa data rights for linear programmer
	if (ctx->scheduler_->is_programmer_linear())
	{
		ctx->scheduler_->init_sa_rights_for_linear_programmer(*ctx->data_right_management_);
	}

	if (initialize_context_audience(*repo, *req, *(ctx->data_privacy_), *ctx, *(ctx->audience_)) < 0)
	{
		ADS_LOG((LP_ERROR, "audience info: failed to initialize context audience\n"));
		// no error return
	}

	if (ctx->scheduler_->is_live_schedule())
	{
		//error return here for logging section and asset information
		if (ctx->scheduler_->has_status(Ads_Scheduler::INITIALIZE_INFO_FAILED))
		{
			ADS_DEBUG((LP_DEBUG, "scheduler failed to initialize info.\n"));
			return -1;
		}
	}

	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_0_1);

	// FW-36208
	if (query_external_info(*ctx) > 0)
		return 1;

	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_0_2);

	if (!ctx->request_->audit_checker_.pass_sivt_detection_check())
	{
		ADS_DEBUG((LP_DEBUG, "request filtered by sivt detection\n"));
		return -1;
	}

	{
		bool is_hhid_from_request = !req->rep()->key_values_.household_id_.empty();
		if (ctx->scheduler_->is_linear_live_schedule())
		{
			// extract syscode for linear, add custom user attrs of audience for qam headend
			const Ads_String &request_content = (req->content() != nullptr ? *(req->content()) : "");
			if (ctx->scheduler_->handle_linear_external_info(*repo, request_content, req->geo_info_, ctx->audience_) < 0)
				return -1;
			ctx->ignore_network_privacy_setting_ = true;
			ctx->request_duration_ = INT_MAX;
		}
		else if (req->id_repo_.household_id().is_valid() && !is_hhid_from_request)
		{
			req->geo_info_.syscode_str_ = ctx->audience_->extract_syscode_from_segments();
			req->geo_info_.syscode_ = ads::str_to_i64(req->geo_info_.syscode_str_);
		}
	}

	if (!req->geo_info_.is_finalized_)
	{
		req->geo_info_.is_finalized_ = true;
		finalize_geo_info(*repo, ctx->network_id_, ctx->scheduler_->is_linear_live_schedule(), req->geo_info_, *(ctx->audience_));
		if (!req->geo_info_.collect_geo_terms(*repo, ctx->root_asset_, req->target_terms_))
			ctx->add_error(Error::GENERAL, Error::GEO_NOT_FOUND);

		ctx->decision_info_.set_values(Decision_Info::INDEX_12, req->geo_source_priority());
		if (req->geo_info_.is_retrieve_by_postal_code())
		{
			ctx->decision_info_.set_flag2(Decision_Info::INFO_2__FLAG_31);
		}
		if (req->geo_info_.country_error())
		{
			ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_11);
		}
	}

	/// global terms
	ctx->global_terms_ = req->target_terms();

	if (!req->rep()->key_values_.po_type_.empty())
		ctx->decision_info_.set_flag2(Decision_Info::INFO_2__FLAG_24);
	if (ctx->scheduler_->is_operator_linear())
	{
		if (ctx->scheduler_->initialize_break_info_for_mvpd(*ctx) < 0)
		{
			ADS_DEBUG((LP_DEBUG, "live linear: failed initialize break info, %s.\n", ctx->scheduler_->debug_info().to_string().c_str()));
			return -1;
		}
#if defined(ADS_ENABLE_LIBADS) && !defined(ADS_ENABLE_FORECAST)
		if (ctx->scheduler_->has_status(Ads_Scheduler::BREAK_SIGNAL_QUERY_NEEDED) && Ads_Server_Config::instance()->aerospike_client_enabled(AEROSPIKE_LINEAR))
		{
			ctx->scheduler_->unset_status(Ads_Scheduler::BREAK_SIGNAL_QUERY_NEEDED);
			ctx->scheduler_->query_break_signal_info(*ctx);
			EXEC_IO("break_signal_lookup");
		}
#endif
	}

	//Move from above to here in order to handling reschedule case when root inventory is not found
	if (ctx->root_asset_ == nullptr)
	{
		ctx->asset_ = nullptr;
	}

	if (ctx->root_section_ == nullptr)
	{
		ctx->section_ = nullptr;
	}
	else if (ctx->is_site_section_from_lw_distributor_)
	{
		//FDB-2199: For LW network distributor, the display inventory belongs to downstream network of video's CRO
		//Move from find_asset_section_distribution_content_right_path
		//to here in order to handling reschedule case under lw network distributor
		ctx->section_ = ctx->root_section_;
		ctx->section_id_ = ctx->root_section_->id_;
		ADS_DEBUG((LP_DEBUG, "rewrite section with root_section: the display inventory that "
			"comes from LW network distributor actually belongs to downstream network of video's CRO\n"));
	}

	/// second check
	if (!ctx->root_asset_ && !ctx->root_section_)
	{
		ctx->add_error(Error::GENERAL, Error::ROOT_ASSET_NOT_FOUND);
		ctx->error(
		    ADS_ERROR_INVALID_REQUEST,
		    ads::Error_Info::SEVERITY_NORMAL,
		    ADS_ERROR_INVALID_REQUEST_S,
		    "",
		    "");

		ADS_ERROR_RETURN((LP_ERROR, "no root asset or root section found in request.\n"), -1);
	}

	/// Till here: root asset OR root section needs to be determined
	const Ads_Network* root_asset_network = nullptr;
	if (ctx->root_asset_ != nullptr)
	{
		repo->find_network(ctx->root_asset_->network_id_, root_asset_network);
	}

	ADS_DEBUG((LP_DEBUG, "root asset network: %s, found: %s\n",
	           ctx->root_asset_ != nullptr ? ADS_ENTITY_ID_CSTR(ctx->root_asset_->network_id_) : "none",
	           root_asset_network != nullptr ? "true" : "false"));

	if (root_asset_network != nullptr)
	{
		ctx->set_forecast_scenario(root_asset_network);
	}

	ctx->load_config_from_networks(*repo, root_asset_network, ctx->section_network_);

	/// kill ad
	ctx->advertisement_blacklist_ = this->advertisement_blacklist_;

	//Only when GEO info initialized, ccpa nf will be check.
	if (!ctx->ignore_network_privacy_setting_ && ctx->data_privacy_->get_ccpa_us_privacy().empty())
	{
		if (ctx->data_privacy_->initialize_data_privacy_ccpa_by_nf(ctx->data_right_management_->dro_network(), req->geo_info_.state_) < 0)
		{
			ADS_LOG((LP_ERROR, "initialize data privacy ccpa by nf fail\n"));
		}
	}
	else
	{
		ADS_DEBUG((LP_DEBUG, "Data Privacy: not check ccpa NF setting\n"));
	}

	/// TODO: excluded industry, move this part to request parser?
	if (!req->rep()->key_values_.excluded_industries_.empty())
	{
		Ads_String_List items;
		if (ads::split(req->rep()->key_values_.excluded_industries_, items, '|') > 0)
		{
			for (Ads_String_List::const_iterator it = items.begin(); it != items.end(); ++it)
			{
				Ads_String s = *it;
				ads::tolower(s);

				Ads_GUID industry_id = repo->advertiser_industry(s.c_str());
				if (ads::entity::is_valid_id(industry_id))
				{
					Ads_GUID_Set industries;
					Ads_Repository::expand_industry_group_to_industry(repo, ctx->delta_repository_, industry_id, industries);
					for (auto industry : industries)
					{
						Ads_GUID industry_id = ads::entity::make_id(ADS_ENTITY_TYPE_ADVERTISER_CATEGORY, industry);
						ctx->restriction_.industry_blacklist_.insert(industry_id);
						ctx->request_->excluded_industries_.insert(industry_id);
					}
				}
				else
				{
					ADS_DEBUG((LP_ERROR, "industry %s not found\n", s.c_str()));
				}
			}
		}
	}

	if (req != nullptr && req->has_device_multiplier())
	{
		ctx->parse_1_to_n_info();
	}

	//set section session context
	if (ctx->section_ != nullptr && !req->rep()->capabilities_.campaign_tag_)
	{
		initialize_website_root(ctx);
	}

	req->id_repo_.update_xifa(ctx->data_privacy_->is_xifa_forbidden(),
	                          ctx->is_ad_framework_integration(),
	                          repo->system_config().xifa_encryption_period_);

	if (!ctx->audience_->is_user_exchange_disabled())
	{
		ctx->audience_->collect_user_exchange_urls(*repo, req->cookies(), ctx->asset_network_, ctx->section_network_);
	}

	/// load CRO audience item
	if (!ctx->data_privacy_->is_audience_targeting_disabled() && ctx->audience_->has_valid_root_network_id())
	{
		auto *closure = ctx->closure(ctx->audience_->root_network_id(), true);
		ADS_DEBUG((LP_DEBUG, "audience info: start to load CRO %s audience items\n", ADS_ENTITY_ID_CSTR(closure->restored_network_id())));

		const Ads_Data_Right &data_right = ctx->data_right_management_->data_right_of_full_network(closure->restored_network_id());
		ctx->audience_->initialize_closure_audience_info(*closure, data_right, closure->mutable_audience_info());
		ctx->audience_->collect_kv_terms(*ctx->repository_, ctx->global_terms_, closure->mutable_audience_info());
		ctx->audience_->transmit_kv_terms(closure->audience_info(), closure->terms_);
		ctx->audience_->collect_audience_items(*repo, closure->mutable_audience_info());
		ctx->audience_->transmit_audience_terms(closure->audience_info(), closure->terms_);
		if (closure->audience_info().server_side_coppa_enabled())
		{
			ADS_DEBUG((LP_DEBUG, "data privacy: with coppa(audience)\n"));
			ctx->data_privacy_->set_coppa();
			ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_25);
		}
	}

	if (ctx->root_asset_ != nullptr)
	{
		/// use asset's language id if not specified in request
		if (!ads::entity::is_valid_id(req->lang_id()) && ads::entity::is_valid_id(ctx->root_asset_->lang_id()))
			ctx->global_terms_.insert(ads::term::make_id(Ads_Term::LANG, ctx->root_asset_->lang_id()));

		/// asset duration
		if (!request_video.duration_.empty())
		{
			ctx->asset_duration_ = ads::str_to_i64(request_video.duration_.c_str());
			if (ctx->asset_duration_ <= 0)
				ctx->error(
				    ADS_ERROR_INVALID_ASSET_DURATION,
				    ads::Error_Info::SEVERITY_WARN,
				    ADS_ERROR_INVALID_ASSET_DURATION_S,
				    "",
				    request_video.duration_);
		}

		if (ctx->asset_duration_ <= 0 && ctx->asset_)
			ctx->asset_duration_ = ctx->asset_->duration();

		if (ctx->asset_duration_ <= 0 && request_video.duration_type_ != "variable")
		{
			if (!(ctx->status_ & STATUS_ASSET_NOT_FOUND))
			ctx->error(
			    ADS_ERROR_ASSET_DURATION_NOT_AVAILABLE,
			    ads::Error_Info::SEVERITY_WARN,
			    ADS_ERROR_ASSET_DURATION_NOT_AVAILABLE_S,
			    "",
			    ctx->asset_ ? ADS_ENTITY_ID_STR(ctx->asset_->id_) : ADS_ENTITY_ID_STR(ctx->asset_id_));
		}
		// request duration
		if (!request_video.request_duration_.empty())
			ctx->request_duration_ = ads::str_to_i64(request_video.request_duration_.c_str());

		if (ctx->request_duration_ <= 0 && ctx->asset_)
		{
			ctx->request_duration_ = ctx->asset_->duration();
			ctx->time_position_ = 0;
		}

		if (ctx->request_duration_ <= 0)
		{
			ctx->add_error(Error::GENERAL, Error::INVALID_REQUEST_DURATION);
			if (!(ctx->status_ & STATUS_ASSET_NOT_FOUND))
				ctx->error(
				    ADS_ERROR_REQUEST_DURATION_NOT_AVAILABLE,
				    ads::Error_Info::SEVERITY_WARN,
				    ADS_ERROR_REQUEST_DURATION_NOT_AVAILABLE_S,
				    "",
				    ads::i64_to_str(ctx->request_duration_));
		}

		if (!request_video.current_time_position_.empty())
			ctx->time_position_ = ads::str_to_i64(request_video.current_time_position_.c_str());
	}

	// for or relation targeting
	ctx->global_terms_.insert(ads::term::make_id(Ads_Term::OR_RELATION, DUMMY_TERM_ID));

	//initialize marketplace standard attribute
	if (ctx->collect_standard_attributes() >= 0)
	{
		ADS_DEBUG((LP_DEBUG, "collected mkpl standard attributes : %s\n", ctx->standard_attributes_.to_string().c_str()));
		ctx->collect_mkpl_global_terms();
	}

	// SLOT generation for ad selection
	ctx->slot_controller_.initialize(*ctx->repository_, *ctx->request_->rep(), ctx->errors_);
	if (ctx->page_slots_.empty() && ctx->player_slots_.empty())
		ctx->slot_controller_.generate_display_slots(ctx->disable_page_slot_delivery_, ctx->page_slots_, ctx->player_slots_, ctx->dot_slot_);
	if (ctx->video_slots_.empty())
		ctx->slot_controller_.generate_video_slots_by_request_rep(ctx->video_slots_, [&req](const Ads_Request::Smart_Rep::Site_Section::Video_Player::Video::Video_Ad_Slot *slot){
			if (req->is_viper_csai_vod())
			{
				if (req->vod_router_ != nullptr && !req->vod_router_->is_slot_need_internal_selection(*slot))
				{
					ADS_DEBUG((LP_DEBUG, "VOD_Router: skip national/entertainment/placement slots, the national ad selection will be done at Canoe side!\n"));
					return false;
				}
			}
			return true;
		});

	if (root_asset_network != nullptr)
	{
		if (req->is_comcast_vod_ws2())
		{
			ctx->flags_ |= Ads_Selection_Context::FLAG_HIRES_TIME_POSITION;
			req->rep()->capabilities_.supports_slot_template_ = true;
			req->rep()->site_section_.video_player_.video_.release_slots();
			std::for_each(ctx->video_slots_.begin(), ctx->video_slots_.end(), std::mem_fun(&Ads_Slot_Base::destroy));
			ctx->video_slots_.clear();
			req->rep()->key_values_.asset_type_ = Ads_Asset_Base::ASSET_CUE_POINT_TYPE::HARDCODED_ADS;
		}

		if (req->is_viper_csai_vod() && req->is_comcast_vod_ws1() && req->vod_router_)
		{
			// some postroll time_position are missing in PO
			req->vod_router_->update_postroll_time_position_with_request_duration(
			    ctx->request_duration_, req->rep()->site_section_.video_player_.video_.slots_, ctx->video_slots_);
		}
		if (!req->is_viper_csai_vod() && req->rep()->site_section_.video_player_.video_.slots_.size() > 0)
		{
			ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_14);
		}

		bool use_cbp = true;
		if (ctx->scheduler_->is_live_schedule())
		{
			// special Turner integration, break&opp could be found, but only player/page slot in request, also need to use cbp
			if (!ctx->video_slots_.empty() && !ctx->scheduler_->has_status(Ads_Scheduler::Status::NEED_FALLBACK_TO_DAI))
			{
				if (ctx->scheduler_->generate_request_video_slot(*ctx, *(ctx->repository_), ctx->video_slots_[0], request_video, use_cbp) < 0)
					return -1;
			}
		}

		/// load slots from templates
		//if (ctx->request_duration() > 0 && req->rep()->capabilities_.supports_slot_template_)
		if (use_cbp && ctx->request_duration() > 0 //FDB-7138
				&& (!(req->is_comcast_vod_ws2() && request_video.movie_paid_.empty()))
				&& (!(req->is_viper_csai_vod() && req->is_comcast_vod_ws1())))
		{
			// collect all terms used for CBP targeting
			Ads_GUID_Set terms_tmp;
			collect_network_terms_for_cbp_targeting(ctx, root_asset_network, terms_tmp);

			// <template_seq, repeat_count>
			std::pair<size_t, size_t> tmp_cookie_record;
			auto cbp = ctx->create_commercial_break_pattern(root_asset_network, std::move(terms_tmp));
			if (!cbp || cbp->find_applicable_slot_template(ctx, root_asset_network, tmp_cookie_record, ctx->decision_info_) < 0)
			{
				ctx->add_error(Error::GENERAL, Error::CBP_NOT_FOUND);
				ADS_DEBUG((LP_DEBUG, "CBP: no applicable cbp found.\n"));
			}

			if (cbp && cbp->slot_template())
			{
				ADS_DEBUG((LP_TRACE, "CBP: using slot template %ld for request duration %d  \n",
						   ads::entity::id(cbp->slot_template()->id_), ctx->request_duration()));
				if (cbp->slot_template_group())
				{
					if (cbp->slot_template_group()->selection_mode_ == Ads_Slot_Template_Group::CYCLE && ctx->request_->o().commercial_pattern_.empty())
					{
						ctx->request_->user_->dirty_flag(Ads_User_Info::COMMERCIAL_PATTERN_HISTORY);
						ctx->request_->user_->add_commercial_pattern_record(ctx->time(), cbp->slot_template()->template_group_id_, tmp_cookie_record);
					}
					// OPP-3425, get trick mode restrictions settings on the template_group
					if (req->is_comcast_vod_ws2())
						req->rep()->trick_mode_restrictions_ = cbp->slot_template_group()->trick_mode_restrictions_;
				}

				cbp->generate_slots_from_template(ctx);
				if (req->rep()->capabilities_.supports_slot_template_ || cbp->is_effective())
				{
					if (cbp->is_effective() && cbp->has_audience_targeting(*ctx))
						ctx->flags_ |= Ads_Selection_Context::FLAG_CBP_AUDIENCE_BILLING;

					ctx->slot_template_id_ = cbp->slot_template()->id_;
					ctx->slot_template_group_id_ = cbp->slot_template()->template_group_id_;
					ctx->slot_template_network_id_ = cbp->slot_template()->network_id_;
				}

				// digital live fallback to CBP
				// TODO: Distinguish has_digital_live_kv and is_digital_live_schedule
				if (ctx->scheduler_->is_digital_live_schedule() && ctx->scheduler_->has_status(Ads_Scheduler::BREAK_NOT_CONFIGURED))
				{
					const Ads_String message = "The break external ID='" + req->rep()->airing_break_custom_id_ + "' is not HYLDA configured, a commercial break pattern has been applied to this break.";
					ctx->error(ADS_ERROR_BREAK_NOT_CONFIGURED_COMMERCIAL_BREAK_PATTERN_APPLIED, ads::Error_Info::SEVERITY_WARN, ADS_ERROR_BREAK_NOT_CONFIGURED_COMMERCIAL_BREAK_PATTERN_APPLIED_S, message, "");
					ctx->add_error(Error::SCHEDULE_INFO, Error::BREAK_NOT_CONFIGURED_CBP_APPLIED);
					ADS_DEBUG((LP_DEBUG, (message + "\n").c_str()));
				}
			}
			else
				ADS_DEBUG((LP_TRACE, "no slot template found for request duration %u, using request slots\n", ctx->request_duration()));
		}
	}

	ctx->slot_controller_.update_request_slots_info(ctx->video_slots_);
	std::copy(ctx->video_slots_.begin(), ctx->video_slots_.end(), std::back_inserter(ctx->request_slots_));
	std::copy(ctx->page_slots_.begin(), ctx->page_slots_.end(), std::back_inserter(ctx->request_slots_));
	std::copy(ctx->player_slots_.begin(), ctx->player_slots_.end(), std::back_inserter(ctx->request_slots_));

	if (ctx->root_asset_ != nullptr && !ctx->is_root_asset_reset_to_ad_router_rovn())
	{
		/// apply light distributor's restrictions
		const Ads_Network *asset_network = ctx->network_;
		if (ctx->asset_ && ctx->asset_->network_id_ != ctx->network_id_)
		{
			if (repo->find_network(ctx->asset_->network_id_, asset_network) >= 0 && asset_network)
			{
				ADS_DEBUG((LP_TRACE, "adding dummy asset from %s\n", ADS_ENTITY_ID_CSTR(ctx->network_id_)));

				Ads_Asset_Base *dummy = 0;
				const Ads_Asset_Base *dasset = 0;

				Ads_GUID_RVector::const_iterator end = ctx->asset_->downstreams_.end();
				for (Ads_GUID_RVector::const_iterator it=ctx->asset_->downstreams_.begin(); it != end; ++it)
				{
					const Ads_Asset_Base *a = 0;
					if (ctx->find_asset(*it, a) >= 0 && a && a->network_id_ == ctx->network_id_ && (a->mirror_type_ & Ads_Asset_Base::MIRROR_DISTRIBUTION))
					{
						dasset = a;
						break;
					}
				}

				if (!dasset)
				{
					dummy = ads::new_object<Ads_Asset_Base>();
					dummy->id_ = ads::entity::invalid_id(ADS_ENTITY_TYPE_ASSET);
					dummy->network_id_ = ctx->network_->id_;
					dasset = dummy;
				}

				ctx->dist_path(ads::MEDIA_VIDEO).push_front(dasset->network_id_);
				ctx->dist_mirrors(ads::MEDIA_VIDEO).push_front(dasset->id_);
				ctx->apply_restrictions(ads::MEDIA_VIDEO, ctx->network_, asset_network, dasset, 0, ctx->network_->run_of_network_id(ads::MEDIA_VIDEO), 0, false, &ctx->restriction(ads::MEDIA_VIDEO));

				if (!dummy) ctx->inventory_info_[dasset->network_id_].first = dasset->id_;
				if (dummy) ads::delete_object(dummy);
			}
		}
	}

	if (ctx->root_section_ != nullptr)
	{
		/// apply light distributor's restrictions
		if (!section ||section->network_id_ != ctx->network_id_)
		{
			const Ads_Network *up_network = 0;
			if (repo->find_network(ctx->section_->network_id_, up_network) >= 0 && up_network)
			{
				ADS_DEBUG((LP_TRACE, "adding dummy section from %s\n", ADS_ENTITY_ID_CSTR(ctx->network_id_)));

				Ads_Asset_Base *dummy = 0;
				const Ads_Asset_Base *dasset = 0;

				Ads_GUID_RVector::const_iterator end = ctx->section_->downstreams_.end();
				for (Ads_GUID_RVector::const_iterator it=ctx->section_->downstreams_.begin(); it != end; ++it)
				{
					const Ads_Asset_Base *a = 0;
					if (ctx->find_section(*it, a) >= 0 && a && a->network_id_ == ctx->network_id_)
					{
						dasset = a;
						break;
					}
				}

				if (!dasset)
				{
					dummy = ads::new_object<Ads_Asset_Base>();
					dummy->id_ = ads::entity::invalid_id(ADS_ENTITY_TYPE_SECTION);
					dummy->network_id_ = ctx->network_->id_;
					dasset = dummy;
				}

				ctx->dist_path(ads::MEDIA_SITE_SECTION).push_front(dasset->network_id_);
				ctx->dist_mirrors(ads::MEDIA_SITE_SECTION).push_front(dasset->id_);
				ctx->apply_restrictions(ads::MEDIA_SITE_SECTION, ctx->network_, up_network, dasset, 0, ctx->network_->run_of_network_id(ads::MEDIA_SITE_SECTION), 0, false, &ctx->restriction(ads::MEDIA_SITE_SECTION));

				if (!dummy) ctx->inventory_info_[dasset->network_id_].second = dasset->id_;
				if (dummy) ads::delete_object(dummy);
			}
		}

		/// display refresh
		ctx->refresh_config_ = this->refresh_config(reinterpret_cast<const Ads_Section_Base *>(ctx->root_section_), ctx->repository_, ctx);
	}

	if (ads::entity::id(ctx->network_->id_) == 10613 || ctx->network_->type_ == Ads_Network::NORMAL)
	{
		ctx->distributor_id_ = ctx->network_->id_;
	}
	else
	{
		const Ads_GUID_List& path =  ctx->dist_path(ctx->asset_? ads::MEDIA_VIDEO: ads::MEDIA_SITE_SECTION);
		Ads_GUID_List::const_iterator it = path.begin();
		if (it != path.end())
			++it;
		if (it != path.end())
		{
			ctx->distributor_id_ = *it;
		}
		else
		{
			ctx->distributor_id_ = (ctx->asset_? ctx->root_network_id(ads::MEDIA_VIDEO): ctx->root_network_id(ads::MEDIA_SITE_SECTION));
		}
	}
	if (ctx->response_ != nullptr)
	{
		ctx->response_->set_distributor_network_id(ctx->distributor_id_);
	}

	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_0_2);
	/// skip ad selection
	if (req->rep()->capabilities_.skips_ad_selection_)
	{
		if (!req->has_explicit_candidates()
	       	&& !(ctx->request_flags() & Ads_Request::Smart_Rep::BYPASS_COMMERCIAL_RATIO)
		    && !req->rep()->capabilities_.bypass_commercial_ratio_)
			this->check_user_experience_commercial_ratio(ctx, req, false);

		return 0;
	}

	ctx->apm_.start(Ads_Selection_Context::APM::CPU_SELECT_0_3);

	// TODO: move to slot controller initialize process
	ctx->user_->get_slots_sequences(ctx->slot_controller_.slot_sequences());
	ctx->slot_controller_.calculate_slot_sequence(ctx->video_slots_, ctx->root_asset_, ctx->user_->sum_slot_sequences(*ctx->repository_));

	if (ctx->root_asset_ != nullptr)
	{
		ADS_DEBUG((LP_DEBUG, "selection: begin to apply restriction/network functions according to root asset %s\n", ADS_ENTITY_ID_CSTR(ctx->root_asset_->id_)));
		/// load tracking-only slots
		if (!ctx->is_tracking() && req->rep()->response_format_ != "ad")		// multiple "ad" requests may cause incorrect counting
		{
			if (ctx->slot_controller_.generate_tracking_slot(ctx->tracking_slot_) >= 0)
			{
				ctx->video_slots_.push_front(ctx->tracking_slot_); // For merge restriction, will pop out later
				ctx->request_slots_.push_front(ctx->tracking_slot_);
			}
		}

		/// apply other restrictions
		this->load_and_apply_restrictions(ctx, ads::MEDIA_VIDEO);

		if (root_asset_network != nullptr)
		{
			if (ctx->is_root_asset_reset_to_ad_router_rovn())
			{
				Ads_GUID ron_id = root_asset_network->run_of_network_id(ads::MEDIA_VIDEO);
				ctx->apply_restrictions(ads::MEDIA_VIDEO, root_asset_network, 0, ctx->root_asset_, 0, ron_id, 0, false, &ctx->restriction(ads::MEDIA_VIDEO)); // Apply routing network asset restriction
			}

			const auto *closure = ctx->closure(root_asset_network->id_);
			//TODO: too many indentation, maybe extract a function for this logic later.
			if (closure != nullptr)
			{
				const auto &audience_info = closure->audience_info();
				if (!audience_info.is_null()) //Restriction can only be set on CRO's audience
				{
					for (const auto& ai : audience_info.audience_items_)
					{
						Ads_Asset_Restriction item_restriction;
						if (Ads_Restriction_Controller::convert_restriction(ctx, ai->id_, root_asset_network, item_restriction) >=0 )
						{
							ctx->apply_restrictions(ads::MEDIA_VIDEO, root_asset_network, 0 /* up_network */, 0 /* asset */, &item_restriction, 0, 0 /* default_restriction */, false, &ctx->restriction_);

							if (ctx->is_watched_info_enabled())
							{
								selection::Restriction::Info &src = ctx->restriction_info_[ai->id_];
								selection::Restriction::Info &dst = ctx->restriction_.info_;
								bool merge[Ads_Asset_Restriction::RESTRICTION_LAST];

								for (size_t i = 0; i < Ads_Asset_Restriction::RESTRICTION_LAST; ++i)
									merge[i] = true;

								Ads_Restriction_Controller::merge_restriction_info(merge, src, dst);
							}
						}
						else
						{
							ADS_DEBUG((LP_DEBUG, "CRO audience restriction convert fail. audience_item_id = %s\n", ADS_ENTITY_ID_CSTR(ai->id_)));
						}
					}
				}
				else
				{
					ADS_DEBUG((LP_DEBUG, "CRO audience restriction is not merged, network_id = %s\n", ADS_ENTITY_ID_CSTR(root_asset_network->id_)));
				}
			}
			else
			{
				ADS_DEBUG((LP_DEBUG, "CRO closure not founded, CRO audience restriction is not merged, network_id = %s\n", ADS_ENTITY_ID_CSTR(root_asset_network->id_)));
			}
		}
	}

	if (ctx->root_section_ != nullptr)
	{
		/// apply other restrictions
		this->load_and_apply_restrictions(ctx, ads::MEDIA_SITE_SECTION);

		//FDB-11543 get comscore demographic data
		// add root section's demographic data into it
		if (ctx->root_section_->comscore_index_ && !ctx->root_section_->comscore_index_->empty())
		{
			ctx->comscore_index_.insert(ctx->root_section_->comscore_index_->begin(), ctx->root_section_->comscore_index_->end());
		}
		// merge ros' demographic data (if not exist in root section)
		for (Ads_GUID_RVector::const_iterator pit = ctx->root_section_->parents_.begin(); pit != ctx->root_section_->parents_.end(); ++pit)
		{
			const Ads_Asset_Base *p_section = 0;
			if (ctx->find_section(*pit, p_section) >= 0 && p_section && p_section->comscore_index_ && !p_section->comscore_index_->empty())
			{
				ctx->comscore_index_.insert(p_section->comscore_index_->begin(), p_section->comscore_index_->end());
			}
		}
	}

	this->merge_advertising_slots_terms(ctx, ctx->request_slots_);

	///FDB-3612 merge slot restriction
	size_t vmask = 0, smask = 0;
	for (size_t i = 0; i < Ads_Asset_Restriction::RESTRICTION_LAST; ++i)
	{
		switch (ctx->restriction(ads::MEDIA_SITE_SECTION).trump_modes_[i])
		{
			case Ads_Asset_Restriction::VIDEO_WIN:
			{
				vmask |= 1 << i;
				ADS_DEBUG((LP_DEBUG, "%s, video axis win, site axis is not merged\n", Ads_Asset_Restriction::to_string(Ads_Asset_Restriction::RESTRICTION_TYPE(i)).c_str()));
				break;
			}
			case Ads_Asset_Restriction::SITE_WIN:
			{
				smask |= 1 << i;
				ADS_DEBUG((LP_DEBUG, "%s, site axis win, video axis is not merged\n", Ads_Asset_Restriction::to_string(Ads_Asset_Restriction::RESTRICTION_TYPE(i)).c_str()));
				break;
			}
			case Ads_Asset_Restriction::TRUMP_NONE:
			case Ads_Asset_Restriction::MERGE:
			default:
			{
				vmask |= 1 << i;
				smask |= 1 << i;
				break;
			}
		}
	}

	if (ctx->is_watched_info_enabled())
	{
		selection::Restriction::Info &src = ctx->restriction(ads::MEDIA_VIDEO).info_;
		selection::Restriction::Info &dst = ctx->restriction_.info_;
		bool merge[Ads_Asset_Restriction::RESTRICTION_LAST];
		for (size_t i = 0; i < Ads_Asset_Restriction::RESTRICTION_LAST; ++i)
			merge[i] = vmask & (1 << i);

		Ads_Restriction_Controller::merge_restriction_info(merge, src, dst);

		src = ctx->restriction(ads::MEDIA_SITE_SECTION).info_;
		for (size_t i = 0; i < Ads_Asset_Restriction::RESTRICTION_LAST; ++i)
			merge[i] = smask & (1 << i);

		Ads_Restriction_Controller::merge_restriction_info(merge, src, dst);
	}

	ctx->restriction_.plus(ctx->restriction(ads::MEDIA_VIDEO), vmask, ads::MEDIA_VIDEO);
	ctx->restriction_.plus(ctx->restriction(ads::MEDIA_SITE_SECTION), smask, ads::MEDIA_SITE_SECTION);
	ctx->restriction_.initialize_global_brand_advertiser_restriction(ctx);
	ADS_DEBUG((LP_DEBUG, "Context global restriction: restrictions=%s\n", ctx->restriction_.debug_info().c_str()));

	for (Ads_Slot_List::iterator it = ctx->request_slots_.begin();
		 it != ctx->request_slots_.end(); ++it)
	{
		Ads_Slot_Base *slot = *it;
		const Ads_Slot_Restriction *restriction = ctx->restriction_.slot_restriction(slot);
		if (!restriction) continue;

		slot->plus_restriction(*restriction);
		if (slot->env() == ads::ENV_VIDEO)
		{
			Ads_Video_Slot *vslot = reinterpret_cast<Ads_Video_Slot *>(slot);

			if (vmask & (1 << Ads_Asset_Restriction::RESTRICTION_AD_UNIT))
			{
				for (std::map<Ads_GUID, Ads_Video_Slot::Ad_Unit_Restriction>::iterator rit = vslot->asset_ad_unit_restrictions_.begin();
					 rit != vslot->asset_ad_unit_restrictions_.end(); ++rit)
				{
					vslot->ad_unit_restrictions_[rit->first].plus(rit->second);
					vslot->ad_unit_restrictions_[rit->first].restriction_ids_.insert(rit->second.restriction_ids_.begin(), rit->second.restriction_ids_.end());
				}
			}
			if (smask & (1 << Ads_Asset_Restriction::RESTRICTION_AD_UNIT))
			{
				for (std::map<Ads_GUID, Ads_Video_Slot::Ad_Unit_Restriction>::iterator rit = vslot->section_ad_unit_restrictions_.begin();
					 rit != vslot->section_ad_unit_restrictions_.end(); ++rit)
				{
					vslot->ad_unit_restrictions_[rit->first].plus(rit->second);
					vslot->ad_unit_restrictions_[rit->first].restriction_ids_.insert(rit->second.restriction_ids_.begin(), rit->second.restriction_ids_.end());
				}
			}
		}
	}
//	FDB-5861 shrink video slot's duration
	if (ctx->slot_duration_shrink_ratio_ < 100 && ctx->slot_duration_shrink_ratio_ >= 0)
	{
		for (Ads_Slot_List::iterator it = ctx->video_slots_.begin(); it != ctx->video_slots_.end(); ++it)
		{
			Ads_Slot_Base *slot = *it;
			Ads_Video_Slot *vslot = reinterpret_cast<Ads_Video_Slot *>(slot);
			if (ads::standard_ad_unit(vslot->time_position_class_.c_str()) == ads::OVERLAY)
				continue;
			if (vslot->max_duration_ == (size_t)-1)
				continue;
			vslot->max_duration_ = vslot->max_duration_ * ctx->slot_duration_shrink_ratio_ / 100;
		}
		for (Ads_Slot_List::iterator it = ctx->parent_slots_.begin(); it != ctx->parent_slots_.end(); ++it)
		{
			Ads_Slot_Base *slot = *it;
			Ads_Video_Slot *vslot = reinterpret_cast<Ads_Video_Slot *>(slot);
			if (ads::standard_ad_unit(vslot->time_position_class_.c_str()) == ads::OVERLAY)
				continue;
			if (vslot->max_duration_ == (size_t)-1)
				continue;
			vslot->max_duration_ = vslot->max_duration_ * ctx->slot_duration_shrink_ratio_ / 100;
		}
	}

	if (!ctx->is_tracking()
#if !defined(ADS_ENABLE_FORECAST)
		&& !ctx->request_->has_explicit_candidates()
#endif
	        && !(ctx->request_flags() & Ads_Request::Smart_Rep::BYPASS_COMMERCIAL_RATIO)
	        && !ctx->request_->rep()->capabilities_.bypass_commercial_ratio_)
		this->check_user_experience_commercial_ratio(ctx, req, true);

	/// FDB-2961: additional restriction assets
	if (!req->rep()->key_values_.additional_restriction_assets_.empty())
	{
		Ads_String_List items;
		if (ads::split(req->rep()->key_values_.additional_restriction_assets_, items, '|') > 0)
		{
			for (Ads_String_List::const_iterator it = items.begin(); it != items.end(); ++it)
			{
				Ads_String s = *it;

				size_t pos = s.find('/');
				if ((pos == Ads_String::npos || !ads::is_numeric(s.substr(0, pos))) && ctx->asset_network_)
					s = ADS_ENTITY_ID_STR(ctx->asset_network_->id_) + "/" + s;

				const Ads_Asset *asset = 0;
				Ads_GUID asset_id = ctx->repository_->lookup_asset_id(ads::tolower(s).c_str(), 0);
				if (ads::entity::is_valid_id(asset_id)
					&& ctx->find_asset(asset_id, asset) >= 0 && asset)
				{
					const Ads_Network *network = 0;
					REPO_FIND_ENTITY_CONTINUE(repo, network, asset->network_id_, network);

					ctx->apply_restrictions(ads::MEDIA_VIDEO, network, 0, asset, 0, 0, 0, false, &ctx->restriction_);

					ctx->additional_negative_terms_.insert(Ads_Asset_Base::term(asset_id));
				}

				if (!asset)
				{
					this->log_selection_asset_not_found(ctx, true /* additional restriction asset */);
					ctx->error(
					    ADS_ERROR_ASSET_NOT_FOUND,
					    ads::Error_Info::SEVERITY_WARN,
					    ADS_ERROR_ASSET_NOT_FOUND_S,
			    		"additional_restriction_assets",
					    s);
				}
			}
		}
	}

	if (ctx->tracking_slot_ != nullptr)
	{
		ADS_ASSERT(ctx->video_slots_.front() == ctx->tracking_slot_);
		if (ctx->video_slots_.front() == ctx->tracking_slot_)
			ctx->video_slots_.pop_front();
		if (ctx->request_slots_.front() == ctx->tracking_slot_)
			ctx->request_slots_.pop_front();
	}

	this->log_no_video_slot(ctx->video_slots_, *ctx);

	if (req->geo_info_.zipcode_error())
	{
		ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_3);
		ctx->error(ADS_ERROR_MALFORMED_ZIPCODE,
				   ads::Error_Info::SEVERITY_INFO,
				   ADS_ERROR_MALFORMED_ZIPCODE_S,
				   "",
				   req->rep()->key_values_.zipcode_);
	}
	ctx->time_state_lookup_ = req->time_state_lookup_;

	if (ctx->scheduler_->has_opportunity())
	{	
		if (ctx->scheduler_->complete_schedule_info(*ctx, ctx->video_slots_, ctx->errors_) < 0)
		{
			ADS_LOG((LP_ERROR, "fail to complete schedule info\n"));
			return -1;
		}
	}

	ctx->fc_->init_frequency_cap(*ctx, ctx->data_privacy_->is_frequency_cap_disabled());

	ctx->auction_mgr_.reset(new selection::Auction_Manager(*ctx));

#if defined(ADS_ENABLE_FORECAST)
	 ctx->fc_->raw_historical_frequency_caps_.insert(ctx->fc_->historical_frequency_caps_.begin(), ctx->fc_->historical_frequency_caps_.end());
	 ctx->fc_->raw_frequency_caps_.insert(ctx->fc_->frequency_caps_.begin(), ctx->fc_->frequency_caps_.end());

#else
	// initialize reject_ads
	bool disable_by_env = !req->is_production_traffic() || ctx->scheduler_->disable_reject_ad();
	ctx->decision_info_.reject_ads_.init(
		ctx->repository_->system_config().reject_reason_profile2sample_ratio_config_,
		ctx->repository_->system_config().reject_reason_network2sample_ratio_config_,
		ctx->repository_->system_config().reject_reason_ad_capacity_,
		ctx->profile_ != nullptr? ctx->profile_->id_: -1,
		ctx->root_network_id(),
		ctx->rand() % 10000,
		disable_by_env);
#endif

	if (ctx->resolve_inventory_split_source() >= 0)
	{
		ADS_DEBUG((LP_DEBUG, "resolved inventory split source : %s\n", ADS_ENTITY_ID_CSTR(ctx->inventory_split_source_id_)));
	}

	set_decision_info(ctx);

	ctx->apm_.end(Ads_Selection_Context::APM::CPU_SELECT_0_3);

	return 0;
}

int
Ads_Selection_Context::load_config_from_networks(const Ads_Repository &repo, const Ads_Network *root_asset_network, const Ads_Network *section_network)
{
	if (root_asset_network != nullptr)
	{
		if (root_asset_network->flags_ & Ads_Network::FLAG_ALLOW_VARIABLE_AD_IN_SPONSORED_SLOT)
			flags_ |= FLAG_EXEMPT_VARIABLE_AD_FOR_SPONSORSHIP;

		const char *s = 0;
		s = repo.network_function("REPEAT_PAYING_AD_FIRST", root_asset_network->id_);
		if (s && s[0] && ads::str_to_i64(s))
			flags_ |= FLAG_REPEAT_PAYING_AD_BEFORE_HOUSE_AD;

		s = repo.network_function("MARGIN_BASED_DELIVERY_CONFIG", root_asset_network->id_);
		if (s && s[0])
		{
			std::vector<Ads_String> items;
			if (ads::split(Ads_String(s), items, ';') > 1)
			{
				default_avoidance_ = ads::str_to_i64(items[0].c_str());
				avoidance_curve_id_ = ads::str_to_i64(items[1].c_str());
			}
		}

		s = repo.network_function("LIMIT_FALLBACK_AD_DURATION", root_asset_network->id_);
		if (s && s[0])
			limit_fallback_ad_duration_ = true;

		if (root_asset_network->config())
		{
			auto it = root_asset_network->config()->partner_avoidance_.find(network_id_);
			if (it != root_asset_network->config()->partner_avoidance_.end())
				avoidance_conf_ = &(it->second);

			const Ads_GUID_List& path = content_right_path(ads::MEDIA_VIDEO);
			if (path.size() && path.back() != root_asset_network->id_)
			{
				it = root_asset_network->config()->partner_avoidance_.find(path.back());
				if (it != root_asset_network->config()->partner_avoidance_.end())
					co_avoidance_conf_ = &(it->second);
			}

			mrm_rule_loop_enabled_ = root_asset_network->config()->enable_mrm_rule_loop_;
			cro_enable_slot_shuffle_ = root_asset_network->config()->enable_slot_shuffle_;
		}

		// distributor revenue share for disabling extended delivery if applicable
		if (network_id_ != root_asset_network->id_)
		{
			const Ads_Revenue_Share_Factor_RMap *revenue_share_factors = root_asset_network->revenue_share_factors(ads::MEDIA_VIDEO, Ads_Network::SHARE_DISTRIBUTOR);
			if (revenue_share_factors)
			{
				const auto it = revenue_share_factors->find(network_id_);
				if (it != revenue_share_factors->end())
					distributor_revenue_share_ = it->second;
			}
		}
	}

	/// front end site section ingestion
	if (section_network != nullptr && (request_status() & Ads_Request::INVALID_CUSTOM_SITE_SECTION_ID))
	{
		const char *s = repo.network_function("FRONTEND_INGEST_SITE_SECTION", section_network->id_);
		if (s && s[0])
		{
			flags_ |= Ads_Selection_Context::FLAG_PROMOTE_SECTION;
		}
	}

	return 0;
}

int Ads_Selector::init_sa_rights_for_stbvod_programmers(const Ads_GUID_Set &stbvod_mkpl_enabled_programmers, Ads_Data_Right_Management &data_right_management)
{
	if (data_right_management.dro_network() == nullptr)
	{
		ADS_LOG((LP_ERROR, "data right network_id is invalid.\n"));
		return -1;
	}	
	Ads_GUID dro_network_id = data_right_management.dro_network()->id_;
	for (const auto programmer_network_id : stbvod_mkpl_enabled_programmers)
	{
		if (programmer_network_id == dro_network_id)
			continue;
		auto &network_right = data_right_management.mutable_data_right_of_full_network(programmer_network_id);
		network_right.set_right(Ads_Data_Right::DATA_FIELD__CONTENT_PROGRAMMER, true);
		network_right.set_right(Ads_Data_Right::DATA_FIELD__CONTENT_FORM, true);
		network_right.set_right(Ads_Data_Right::DATA_FIELD__CONTENT_LANGUAGE, true);
		network_right.set_right(Ads_Data_Right::DATA_FIELD__ENDPOINT, false);
		network_right.set_right(Ads_Data_Right::DATA_FIELD__ENDPOINT_OWNER, false);
	}
	return 0;
}


void Ads_Selector::set_decision_info(Ads_Selection_Context *ctx)
{
	const Ads_Request *req = ctx->request_;
	if (req->ua_ && req->ua_->device_.name_.empty() && !req->ua_->is_desktop())
		ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_27);

	if (req->ua_ && req->ua_->os_.name_.empty() && req->ua_->genos_.empty())
		ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_28);

	if (req->enable_ssus() && !req->is_ssus_get_valid())
		ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_30);

	if (req->rep()->capabilities_.synchronize_multiple_requests_)
		ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_9);

	if (req->enable_ssus())
		ctx->decision_info_.set_flag2(Decision_Info::INFO_2__FLAG_21);

	if (req->cookies_.size() > 0)
		ctx->decision_info_.set_flag2(Decision_Info::INFO_2__FLAG_22);

	if (req->user() != nullptr)
	{
		if (req->user()->ad_views_over_sizes_)
			ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_4);
		if (req->user()->stream_ad_views_over_sizes_)
			ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_15);
		if (req->user()->asset_ad_views_over_sizes_)
			ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_16);
		if (req->user()->section_ad_views_over_sizes_)
			ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_17);
	}

	if (req->cut_to_new_ua_)
		ctx->decision_info_.set_flag2(Decision_Info::INFO_2__FLAG_1);

	if (req->is_fallback_ua_platform_group_)
		ctx->decision_info_.set_flag2(Decision_Info::INFO_2__FLAG_14);

	if (ctx->data_privacy_->is_gdpr_v2_honor_core_tc())
		ctx->decision_info_.set_flag2(Decision_Info::INFO_2__FLAG_18);

	if (ctx->data_privacy_->is_gdpr_v2_publisher_purpose_always_on())
		ctx->decision_info_.set_flag2(Decision_Info::INFO_2__FLAG_2);

	if (req->need_samesite())
		ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_10);

	if (!req->id_repo_.ssus_key().empty() && req->enable_ssus())
	{
		if (req->is_ssus_flag_dirty())
			ctx->decision_info_.set_flag2(Decision_Info::INFO_2__FLAG_25);
	}

	if (req->is_viper_csai_vod() && req->vod_router_ != nullptr)
	{
		if (req->vod_router_->is_asset_po_used())
			ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_21);
		else if (req->vod_router_->is_asset_cue_point_used())
			ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_22);
		else
			ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_23);
	}

	if (req->id_repo_.universal_hhid().is_valid())
		ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_5);
	if (req->id_repo_.household_id().is_valid())
		ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_6);

	if (is_ptiling_request(*req, ctx->page_slots_))
		ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_7);
	if (req->linktag_request())
		ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_8);

	if (!req->has_valid_geo_info_parsed() && !ctx->scheduler_->is_linear_live_schedule())
		ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_18);

	if (req->anonymous())
		ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_22);
}

void Ads_Selector::log_reject_reason(Ads_Selection_Context &ctx) const
{
	ADS_DEBUG((LP_TRACE, "reject_reason: log reject reason ad num %ld\n", ctx.decision_info_.reject_ads_.get_reject_ads().size()));
	Ads_Monitor::stats_set(Ads_Monitor::REJECT_AD, ctx.decision_info_.reject_ads_.get_reject_ads().size());
	for (const auto& reject_ad : ctx.decision_info_.reject_ads_.get_reject_ads())
	{
		auto it = ctx.restricted_candidates_logs_.emplace(reject_ad.id_, reject_ad.reject_reason());
		reject_ad.store_sub_reasons(it->second.sub_reasons_);
	}
}

void Ads_Selector::log_performance_info(Ads_Selection_Context &ctx) const
{
	if (Ads_Server_Config::instance()->enable_performance_debug_ == false)
		return;

	int p0_duration = ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_CPU, Ads_Selection_Context::APM::CPU_SELECT_0_1) +
						ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_CPU, Ads_Selection_Context::APM::CPU_SELECT_0_2) +
						ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_CPU, Ads_Selection_Context::APM::CPU_SELECT_0_3);
	int p1_duration = ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_CPU, Ads_Selection_Context::APM::CPU_SELECT_1_0) +
						ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_CPU, Ads_Selection_Context::APM::CPU_SELECT_1_1);
	int p2_duration = ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_CPU, Ads_Selection_Context::APM::CPU_SELECT_2_0);
	int p3_duration = ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_CPU, Ads_Selection_Context::APM::CPU_SELECT_3_0);
	int p4_duration = ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_CPU, Ads_Selection_Context::APM::CPU_SELECT_4_0);
	int p6_duration = ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_CPU, Ads_Selection_Context::APM::CPU_SELECT_6_0);
	int p7_duration = ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_CPU, Ads_Selection_Context::APM::CPU_SELECT_7_0);
	int p8_duration = ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_CPU, Ads_Selection_Context::APM::CPU_SELECT_8_0) +
						ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_CPU, Ads_Selection_Context::APM::CPU_SELECT_8_1);
	int p9_duration = ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_CPU, Ads_Selection_Context::APM::CPU_SELECT_9_0);
	int p10_duration = ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_CPU, Ads_Selection_Context::APM::CPU_SELECT_10_0);
	int p11_duration = ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_CPU, Ads_Selection_Context::APM::CPU_SELECT_11_0);
	int time_selection = ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_CPU, Ads_Selection_Context::APM::TASK_TYPE_NONE);
	int internal_io = ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_INTERNAL_IO, Ads_Selection_Context::APM::TASK_TYPE_NONE);
	int external_io = ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_EXTERNAL_IO, Ads_Selection_Context::APM::TASK_TYPE_NONE);
	int time_response = ctx.apm_.get_duration_us(Ads_Selection_Context::APM::DURATION_RESPONSE, Ads_Selection_Context::APM::TASK_TYPE_NONE);

	int profile_id = 0;
	if (ctx.profile_)
	{
		profile_id = ctx.profile_->id_;
	}

	ADS_LOG((LP_INFO, "performance tool usage for duration: profile_id:%d; p0:%f; p1:%f; p2:%f; p3:%f; p4:%f; p6:%f; p7:%f; p8:%f; p9:%f; p10:%f; p11:%f; time_selection:%f; internal_io:%f; external_io:%f; time_response:%f\n",
			profile_id,
			p0_duration * 1.0 / 1000,
			p1_duration * 1.0 / 1000,
			p2_duration * 1.0 / 1000,
			p3_duration * 1.0 / 1000,
			p4_duration * 1.0 / 1000,
			p6_duration * 1.0 / 1000,
			p7_duration * 1.0 / 1000,
			p8_duration * 1.0 / 1000,
			p9_duration * 1.0 / 1000,
			p10_duration * 1.0 / 1000,
			p11_duration * 1.0 / 1000,
			time_selection * 1.0 / 1000,
			internal_io * 1.0 / 1000,
			external_io * 1.0 / 1000,
			time_response * 1.0 / 1000));

	ctx.insight_info_.append("p6_closure_num", ctx.decision_info_.values(Decision_Info::INDEX_6));
	ctx.insight_info_.append("slot_num", ctx.decision_info_.values(Decision_Info::INDEX_5));
	ctx.insight_info_.append("p6_plc_num", ctx.decision_info_.values(Decision_Info::INDEX_7));
	ctx.insight_info_.append("p6_ad_num", ctx.decision_info_.values(Decision_Info::INDEX_9));
	ctx.insight_info_.append("p7_ad_num", ctx.decision_info_.values(Decision_Info::INDEX_10));
	ctx.insight_info_.append("p8_ad_num", ctx.decision_info_.values(Decision_Info::INDEX_11));
	ctx.insight_info_.append("p9_ad_num", ctx.decision_info_.values(Decision_Info::INDEX_24));
	ADS_LOG((LP_INFO, "performance tool usage for insight: profile_id:%d; %s\n", profile_id, ctx.insight_info_.to_string().c_str()));
}

int
Ads_Selector::lookup_source_signal_break_cache(const Ads_String &key, Ads_String &value)
{
	Source_Signal_Break_Cache::GET_RESULT result = source_signal_break_cache_.get(key, value);
	if (result == Source_Signal_Break_Cache::GET_RESULT::ATC_GET_FOUND)
	{
		ADS_DEBUG((LP_DEBUG, "Linear source signal break cache: get success, key: %s\n", key.c_str()));
		Ads_Monitor::stats_inc(Ads_Monitor::SOURCE_SIGNAL_BREAK_LOCAL_CACHE_HIT);
		return 0;
	}
	//result == Source_Signal_Bteak_Cache::GET_RESULT::ATC_GET_NOT_FOUND
	ADS_DEBUG((LP_DEBUG, "Linear source signal break cache: get fail, key %s not found\n", key.c_str()));
	Ads_Monitor::stats_inc(Ads_Monitor::SOURCE_SIGNAL_BREAK_LOCAL_CACHE_MISS);
	return -1;
}

int
Ads_Selector::update_source_signal_break_cache(const Ads_String &key, const Ads_String &value)
{
	Source_Signal_Break_Cache::SET_RESULT result = source_signal_break_cache_.set(key, value, source_signal_break_cache_timeout_, false/*don't overwrite*/);
	if (result == Source_Signal_Break_Cache::SET_RESULT::ATC_SET_OK)
	{
		ADS_DEBUG((LP_DEBUG, "Linear source signal break cache: set success, key %s\n", key.c_str()));
		Ads_Monitor::stats_inc(Ads_Monitor::SOURCE_SIGNAL_BREAK_LOCAL_CACHE_SET_OK);
		return 0;
	}
	else if (result == Source_Signal_Break_Cache::SET_RESULT::ATC_SET_OUT_OF_CACHE_CAPACITY)
	{
		ADS_LOG((LP_ERROR, "Linear source signal break cache: set fail, key %s\n", key.c_str()));
		Ads_Monitor::stats_inc(Ads_Monitor::SOURCE_SIGNAL_BREAK_LOCAL_CACHE_NO_SPACE);
		return -1;
	}
	else // if (result == Source_Signal_Break_Cache::SET_RESULT::ATC_SET_KEY_ALREADY_EXIST)
	{
		ADS_DEBUG((LP_DEBUG, "Linear source signal break cache: key %s already exist\n", key.c_str()));
		Ads_Monitor::stats_inc(Ads_Monitor::SOURCE_SIGNAL_BREAK_LOCAL_CACHE_KEY_EXIST);
		return -1;
	}
}

int
Ads_Selector::lookup_signal_break_cache(const Ads_String &key, std::pair<Ads_GUID, time_t> &value)
{
	Signal_Break_Cache::GET_RESULT result = this->signal_break_cache_.get(key, value);
	if (result == Signal_Break_Cache::GET_RESULT::ATC_GET_FOUND)
	{
		ADS_DEBUG((LP_DEBUG, "Linear signal break cache: get success, key: %s, value: (%s, %s)\n", key.c_str(),
			ADS_ENTITY_ID_CSTR(value.first), ads::i64_to_str(value.second).c_str()));
		Ads_Monitor::stats_inc(Ads_Monitor::SIGNAL_BREAK_LOCAL_CACHE_HIT);
		return 0;
	}
	// result == Signal_Bteak_Cache::GET_RESULT::ATC_GET_NOT_FOUND
	ADS_DEBUG((LP_DEBUG, "Linear signal break cache: get fail, key %s not found\n", key.c_str()));
	Ads_Monitor::stats_inc(Ads_Monitor::SIGNAL_BREAK_LOCAL_CACHE_MISS);
	return -1;
}

int
Ads_Selector::update_signal_break_cache(const Ads_String &key, const std::pair<Ads_GUID, time_t>& value)
{
	Signal_Break_Cache::SET_RESULT result = this->signal_break_cache_.set(key, value, this->signal_break_cache_timeout_, false/*don't overwrite*/);
	if (result == Signal_Break_Cache::SET_RESULT::ATC_SET_OK)
	{
		ADS_DEBUG((LP_DEBUG, "Linear signal break cache: set success, key %s, value: (%s, %s)\n", key.c_str(),
			ADS_ENTITY_ID_CSTR(value.first), ads::i64_to_str(value.second).c_str()));
		Ads_Monitor::stats_inc(Ads_Monitor::SIGNAL_BREAK_LOCAL_CACHE_SET_OK);
		return 0;
	}
	else if (result == Signal_Break_Cache::SET_RESULT::ATC_SET_OUT_OF_CACHE_CAPACITY)
	{
		ADS_LOG((LP_ERROR, "Linear signal break cache: set fail, key %s, value: (%s, %s)\n", key.c_str(),
			ADS_ENTITY_ID_CSTR(value.first), ads::i64_to_str(value.second).c_str()));
		Ads_Monitor::stats_inc(Ads_Monitor::SIGNAL_BREAK_LOCAL_CACHE_NO_SPACE);
		return -1;
	}
	else // if (result == Signal_Break_Cache::SET_RESULT::ATC_SET_KEY_ALREADY_EXIST)
	{
		ADS_DEBUG((LP_DEBUG, "Linear signal break cache: key %s already exist\n", key.c_str()));
		Ads_Monitor::stats_inc(Ads_Monitor::SIGNAL_BREAK_LOCAL_CACHE_KEY_EXIST);
		return -1;
	}
}

void
Ads_Selector::update_none_syncing_creative_view_record(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate *candidate, const Ads_Creative_Rendition *creative)
{
	if (ctx == nullptr || ctx->user_ == nullptr
		|| candidate == nullptr || candidate->ad_ == nullptr || creative == nullptr)
	{
		ADS_LOG((LP_ERROR, "ctx, user, candidate, ad, creative is nullptr!\n"));
		return;
	}

	if (candidate->ad_->rotation_type_ == Ads_Advertisement::SEQUENCE)
	{
		int rf = candidate->ad_->rotation_factor(creative->creative_id_);
		if (rf >= 0)
		{
			if (!ctx->is_prefetch())//request without callback, flush into userinfo immediately.
			{
				ctx->user_->add_creative_view_record(ctx->request_->time_created().sec(), candidate->ad_->id_, rf);
			}
			else//temporary update for current request.
			{
				ctx->advertisement_creative_sequence_[candidate->ad_->id_] = rf;
			}
		}
	}
}

bool
Ads_Selector::is_vod_primary_asset_location_valid(const Ads_String &url)
{
	if (url.empty())
		return false;
	Ads_String prefix = url.substr(0, 6);
	ads::tolower(prefix);
	if (prefix != "vod://")
		return false;
	size_t pos = url.find('/', 6);
	if (pos == Ads_String::npos)
		return false;
	return true;
}

int
Ads_Selector::find_asset_section_distribution_content_right_path(Ads_Selection_Context *ctx, ads::MEDIA_TYPE assoc, const Ads_Asset_Base *asset, const Ads_Asset_Base *&root_asset, Ads_GUID_List& asset_dist_path, Ads_GUID_List& asset_dist_mirrors, Ads_GUID_List& asset_content_right_path, Ads_GUID_List& asset_content_right_mirrors, bool apply_restriction)
{
	if (asset == nullptr || ctx == nullptr || ctx->repository_ == nullptr)
	{
		ADS_LOG((LP_ERROR, "asset[%p] or ctx[%p] or ctx->repository_ is nullptr.\n", asset, ctx));
		return -1;
	}
	const Ads_Repository *repo = ctx->repository_;
	const size_t MAX_ITERATION = 10000;
	size_t n_iteration = 0;

	while (asset->mirror_type_ != 0 
		&& !(asset->mirror_type_ & Ads_Asset_Base::MIRROR_COPYRIGHT) 
		&& !(asset->flags_ & Ads_Asset_Base::FLAG_ROS /* ROS could be the fallback root section */))
	{
		///XXX: circle detection
		if (++ n_iteration >= MAX_ITERATION)
		{
			ADS_ERROR_RETURN((LP_ERROR, "circle dected for asset %s.\n", ADS_ASSET_ID_CSTR(asset->id_)), -1);
		}

		if (asset->is_pending())
			break;

		if (!(asset->mirror_type_ & Ads_Asset_Base::MIRROR_DISTRIBUTION))
		{
			ADS_ERROR_RETURN((LP_ERROR, "non distributable asset %s.\n" , ADS_ASSET_ID_CSTR(asset->id_)), -1);
		}

		if (!ads::entity::is_valid_id(asset->upstream_))
		{
			ADS_ERROR_RETURN((LP_ERROR, "no CRO for asset %s.\n" , ADS_ASSET_ID_CSTR(asset->id_)), -1);
		}

		asset_dist_path.push_back(asset->network_id_);
		asset_dist_mirrors.push_back(asset->id_);

		if (assoc == ads::MEDIA_VIDEO)
		{
			if (!apply_restriction) 
				ctx->inventory_info_[asset->network_id_].first = asset->id_;
		}
		else
			ctx->inventory_info_[asset->network_id_].second = asset->id_;

		if (asset->upstream_network_id_ == asset->network_id_)
		{
			ADS_ERROR_RETURN((LP_ERROR, "asset %s and upstream %s has the same network id\n", ADS_ASSET_ID_CSTR(asset->id_), ADS_ASSET_ID_CSTR(asset->upstream_)), -1);
		}

		const Ads_Network *up_network = 0, *network = 0;
		REPO_FIND_ENTITY_RETURN(repo, network, asset->upstream_network_id_, up_network, -1);
		REPO_FIND_ENTITY_RETURN(repo, network, asset->network_id_, network, -1);

		if (apply_restriction)
		{
			ctx->apply_restrictions(assoc, network, up_network, asset, 0, network->run_of_network_id(assoc), 0, false, &ctx->restriction(assoc));
		}

		const Ads_Asset_Base *up_asset = 0;
		if (ctx->find_asset_or_section(asset->upstream_, up_asset) < 0 || !up_asset)
			// upsteam is lightweight CO mirror
			break;

		asset = up_asset;
	}

	//TODO
	//FDB-2199: For LW network distributor, the display inventory is belongs to downstream network of video's CRO
	if (assoc == ads::MEDIA_SITE_SECTION)
	{
		const Ads_Network *network = 0;
		if (ctx->root_asset_ &&
		        ctx->repository_->find_network(asset->network_id_, network) >= 0 && network
		        && network->type_ == Ads_Network::LIGHT)
		{
			ctx->is_site_section_from_lw_distributor_ = true;
			const Ads_Asset_Base *downstream = this->downstream_p(asset, ctx->repository_, ctx->root_asset_->network_id_, ctx);

			if (downstream)
			{
				///XXX: also add it to distribution chain
				if (asset_dist_path.empty())
				{
					asset_dist_path.push_back(asset->network_id_);
					asset_dist_mirrors.push_back(asset->id_);
				}

				asset = downstream;
				ctx->section_ = asset;
				ctx->section_id_ = asset->id_;
			}
			else // fallback to ROS
			{
				const Ads_Network* root_asset_network = 0;
				if (repo->find_network(ctx->root_asset_->network_id_, root_asset_network) >= 0 && root_asset_network)
				{
					const Ads_Asset_Base *dsection = 0;
					Ads_GUID dsection_id = root_asset_network->upstream_network_ron_mirror_id(ads::MEDIA_SITE_SECTION, network->id_);
					if (ads::entity::is_valid_id(dsection_id) && ctx->find_section(dsection_id, dsection) >= 0 && dsection)
					{
						///XXX: also add it to distribution chain
						if (asset_dist_path.empty())
						{
							asset_dist_path.push_back(asset->network_id_);
							asset_dist_mirrors.push_back(asset->id_);
						}

						asset = dsection;
						ctx->section_ = asset;
						ctx->section_id_ = asset->id_;
					}
				}
			}
		}
	}

	root_asset = asset;

	if (assoc == ads::MEDIA_VIDEO)
	{
		if (!apply_restriction) 
			ctx->inventory_info_[asset->network_id_].first = asset->id_;
	}
	else
		ctx->inventory_info_[asset->network_id_].second = asset->id_;

	n_iteration = 0;
	while (ads::entity::is_valid_id(asset->upstream_network_id_) && asset->mirror_type_ != 0)
	{
		///XXX: circle detection
		if (++ n_iteration >= MAX_ITERATION)
		{
			ADS_ERROR_RETURN((LP_ERROR, "circle dected for asset %s.\n", ADS_ASSET_ID_CSTR(asset->id_)), -1);
		}

		if (asset->upstream_network_id_ == asset->network_id_)
		{
			ADS_DEBUG((LP_ERROR, "asset %s and upstream %s has the same network id\n",
			           ADS_ASSET_ID_CSTR(asset->id_),
			           ADS_ASSET_ID_CSTR(asset->upstream_)));
			return -1;
		}

		asset_content_right_path.push_back(asset->upstream_network_id_);
		asset_content_right_mirrors.push_back(asset->upstream_);

		if (assoc == ads::MEDIA_VIDEO)
		{
			if (!apply_restriction) 
				ctx->inventory_info_[asset->upstream_network_id_].first = asset->upstream_;
		}
		else
			ctx->inventory_info_[asset->upstream_network_id_].second = asset->upstream_;

		if (apply_restriction)
		{
			const Ads_Network *up_network = 0, *network = 0;
			REPO_FIND_ENTITY_RETURN(repo, network, asset->upstream_network_id_, up_network, -1);
			REPO_FIND_ENTITY_RETURN(repo, network, asset->network_id_, network, -1);

			const Ads_Asset_Base *rasset = asset;
			if (ctx->scheduler_->is_programmer_schedule() && (assoc == ads::MEDIA_VIDEO))
			{
				//For digital live request, if asset upstream is lightweight CO and asset's network is cro network, airing restriction has higher priority than asset, so replace asset to airing here before apply restriction.
				if (ctx->scheduler_->airing_ != nullptr && ctx->scheduler_->cro_network_id_ == asset->network_id_)
				{
					rasset = ctx->scheduler_->airing_;
				}
			}
			ctx->apply_restrictions(assoc, network, up_network, rasset, 0, network->run_of_network_id(assoc), 0, false, &ctx->restriction(assoc));
		}

		const Ads_Asset_Base *up_asset = 0;
		if (ctx->find_asset_or_section(asset->upstream_, up_asset) < 0 || !up_asset)
			// upsteam is lightweight CO mirror
			break;

		asset = up_asset;
	}

	/// the end point
	{
		if (apply_restriction && !ads::entity::is_valid_id(asset->upstream_network_id_))
		{
			const Ads_Network *network = 0;
			REPO_FIND_ENTITY_RETURN(repo, network, asset->network_id_, network, -1);

			const Ads_Asset_Base *rasset = asset;
			if (ctx->scheduler_->is_programmer_schedule() && (assoc == ads::MEDIA_VIDEO))
			{
				//For digital live request, if asset has no upstream, because airing restriction has higher priority than asset in cro network, so replace asset to airing here before apply restriction.
				if (ctx->scheduler_->airing_ != nullptr && ctx->scheduler_->cro_network_id_ == asset->network_id_)
				{
					rasset = ctx->scheduler_->airing_;
				}
			}
			ctx->apply_restrictions(assoc, network, 0, rasset, 0, network->run_of_network_id(assoc), 0, false, &ctx->restriction(assoc));
		}
	}

	return 0;
}

bool
Ads_Selector::test_candidate_advertisement(
		Ads_Selection_Context& ctx,
		const Ads_Advertisement_Candidate_Lite& ad_lite,
		bool& sponsored)
{
	return ad_lite.ad() != nullptr &&
	       test_advertisement(ctx, *ad_lite.ad(), ad_lite.owner_, sponsored) &&
	       test_advertisement_candidate_lite(ctx, ad_lite);
}

bool
Ads_Selector::test_advertisement_candidate_lite(
		Ads_Selection_Context& ctx,
		const Ads_Advertisement_Candidate_Lite& ad_lite)
{
	bool is_watched = ctx.is_watched(ad_lite);

//	if (! ad->is_sponsorship())
	{
		if (!ad_lite.is_compliance_check_enabled_
				&& !ad_lite.test_advertisement_blacklist(NULL, NULL, &ctx.restriction_.industry_blacklist_, NULL, NULL, NULL, NULL, NULL, NULL,
					&ctx.restriction_.global_brand_blacklist_, &ctx.restriction_.global_brand_whitelist_, &ctx.restriction_.global_advertiser_blacklist_, &ctx.restriction_.global_advertiser_whitelist_,
					ctx.restriction_))
		{
			ctx.decision_info_.add_values(Decision_Info::INDEX_19);
			if (is_watched)
				ctx.log_watched_info(ad_lite, "RESTRICTION_BLACKLIST");
			return false;
		}

		if (!this->test_advertisement_external_access_rule(&ctx, &ad_lite))
		{
			ctx.decision_info_.add_values(Decision_Info::INDEX_17);
			if (is_watched)
				ctx.log_watched_info(ad_lite, "EXTERNAL_ACCESS_RULE");
			return false;
		}

		if (ctx.status_ & STATUS_ASSET_PROMO_ONLY)
		{
			if (!ad_lite.is_promo())
			{
				if (is_watched)
					ctx.log_watched_info(ad_lite, "PROMO_ONLY");
				return false;
			}
		}
	}
	return true;
}

bool
Ads_Selector::test_advertisement(Ads_Selection_Context& ctx, const Ads_Advertisement& ad, selection::Advertisement_Owner& ad_owner, bool& sponsored)
{
	bool is_watched = ctx.is_watched(ad);
	// ignore root CRO bumper
	if (ad.is_bumper())
	{
		if(ctx.root_asset_ && ctx.root_asset_->network_id_ == ad.network_id_ && ad.is_active(ctx.time(), ctx.enable_testing_placement()))
		{
			ADS_DEBUG((LP_DEBUG, "ad %d is cro bumper: ignore some checking\n", ADS_ENTITY_ID_CSTR(ad.id_)));
		}
		else
			return false;
	}
	else  // no cro bumper: do these checking
	{
		//TODO: remove?
		if (!ad.nonapplicable_distributors_.empty()
			&& ad.nonapplicable_distributors_.find(ctx.network_id_) != ad.nonapplicable_distributors_.end())
		{
			ADS_DEBUG((LP_DEBUG,
					"network %s not in ad %s's targeted distributors\n"
					, ADS_ENTITY_ID_CSTR(ctx.network_id_)
					, ADS_ENTITY_ID_CSTR(ad.id_)));
			return false;
		}

		// Some HyLDA clients created thousands of placemnts(DISABLE DAI) to serve live events, e.g. Seven One Media
		// To improve Adserver performance, we filter out these ads during targeting check
		// This change will not impact its delivery, because Adserver could deliver schedule ads successfully even targeting check is fail
		if (ad.is_scheduled_only())
		{
			ctx.decision_info_.add_values(Decision_Info::INDEX_14);
			if (is_watched)
				ctx.log_watched_info(ad, "SCHEDULED_ONLY");
			return false;
		}
		///execute selection filters
		const ads::Status& status = ad.check_activity(ctx.time(), ctx.enable_testing_placement());
		if (status != Ads_Advertisement::ACTIVE)
		{
			ctx.decision_info_.add_values(Decision_Info::INDEX_15);
			if (is_watched)
			{
				ctx.log_watched_info(ad, status.message());
			}
			return false;
		}
		if (this->is_advertisement_in_blacklist(&ctx, &ad))
		{
			ctx.decision_info_.add_values(Decision_Info::INDEX_16);
			if (is_watched)
				ctx.log_watched_info(ad, "BLACKLIST");
			return false;
		}
		//TODO: deprecate this feature
		if (!this->test_comscore_targeting(&ctx, &ad))
		{
			if (is_watched)
				ctx.log_watched_info(ad, "COMSCORE_TARGETING");
			return false;
		}
		// check campaign privacy restriction setting to filter ads if necessary
		if (ad.is_data_privacy_restriction_enabled() && ctx.data_privacy_->is_ad_filtered_by_privacy())
		{
			ctx.decision_info_.add_values(Decision_Info::INDEX_23);
			ADS_DEBUG((LP_DEBUG, "ad %s is filtered by data privacy restriction\n", ADS_ENTITY_ID_CSTR(ad.id_)));
			if (is_watched)
				ctx.log_watched_info(ad, "FILTERD_BY_DATA_PRIVACY");
			return false;
		}
	}

	// assign sponsored after test_advertisement_history_exclusivity, if a sponsored ad is filtered by hisotry_exclusivity, then this ad will be treated as non-sponsored ad
	// if assign sponsored before test_advertisement_history_exclusivity, 2 reg case HYLDA-ADS-551/HYLDA-ADS-552 will fail
	if (!this->test_advertisement_history_exclusivity(&ctx, &ad))
	{
		ctx.decision_info_.add_values(Decision_Info::INDEX_18);
		if (is_watched)
			ctx.log_watched_info(ad, "HISTORY_EXCLUSIVITY");
		return false;
	}
	if (ad.is_sponsorship()) sponsored = true;

	if (!test_advertisement_blacklist_i(&ctx, ad, ad_owner, &ctx.restriction_.advertiser_blacklist_, &ctx.restriction_.advertiser_whitelist_, NULL, &ctx.restriction_.reseller_blacklist_, &ctx.restriction_.reseller_whitelist_, &ctx.restriction_.targeting_inventory_blacklist_, &ctx.restriction_.targeting_inventory_whitelist_, &ctx.restriction_.brand_blacklist_, &ctx.restriction_.brand_whitelist_, ctx.restriction_))
	{
		if (is_watched)
			ctx.log_watched_info(ad, "RESTRICTION_BLACKLIST");
		return false;
	}

#if !defined(ADS_ENABLE_FORECAST)
	/// TODO: fix MRM-7295
	if (!this->test_advertisement_rating(&ctx, &ad))
	{
		ctx.decision_info_.add_values(Decision_Info::INDEX_20);
		ctx.mkpl_executor_->log_ad_error(&ctx, ad, ad_owner, selection::Error_Code::RATING_RESTRICTION);
		if (is_watched)
			ctx.log_watched_info(ad, "RATING " + ctx.restriction_.info_.to_string(&ctx, Ads_Asset_Restriction::RESTRICTION_RATING, -1));

		return false;
	}
#endif

	return true;
}

bool
Ads_Selector::test_advertisement_blacklist_i(Ads_Selection_Context *ctx, const Ads_Advertisement &ad, selection::Advertisement_Owner &ad_owner,
					const Ads_GUID_Set *advertiser_blacklist, const std::map<Ads_GUID, Ads_GUID_Set> *advertiser_whitelist,
					const Ads_GUID_Set *industry_blacklist, const Ads_GUID_Set *reseller_blacklist, const Ads_GUID_Set *reseller_whitelist,
					const Ads_GUID_Set *targeting_inventory_blacklist, const Ads_GUID_Set *targeting_inventory_whitelist,
					const Ads_GUID_Set *brand_blacklist, const std::map<Ads_GUID, Ads_GUID_Set> *brand_whitelist,
					const selection::Restriction &restriction, const Ads_Advertisement_Candidate_Lite *ad_lite/* = nullptr */)
{
	bool verbose = ctx->verbose_ > 0;
	bool is_watched = false, looped = false;
	if (ad_lite == nullptr)
	{
		is_watched = ctx->is_watched(ad);
		const auto *closure = ad_owner.to_closure();
		looped = (closure != nullptr && closure->is_looped_seller());
	}
	else
	{
		is_watched = ctx->is_watched(*ad_lite);
		looped = ad_lite->is_looped();
	}

	if (brand_blacklist)
	{
		const Ads_GUID_Set &blacklist = *brand_blacklist;
		if (blacklist.find(ad.brand_) != blacklist.end())
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE, "%s failed (brand %ld, blacklist)\n", ADS_ADVERTISEMENT_LOOPED_ID_CSTR(ad.id_, looped), ads::entity::id(ad.brand_)));
			if (is_watched && ad_lite == nullptr)
				ctx->log_watched_info(ad, "RESTRICTION_BRAND_BLACKLIST " + restriction.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_BRAND, ad.brand_));
			else if (is_watched && ad_lite != nullptr)
				ctx->log_watched_info(*ad_lite, "RESTRICTION_BRAND_BLACKLIST " + restriction.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_BRAND, ad_lite->ad_->brand_));

			ctx->add_restricted_candidate_log(ad.id_, "BRAND_IN_BLACK_LIST");
			ctx->mkpl_executor_->log_ad_error(ctx, ad, ad_owner, selection::Error_Code::BRAND_BLACKLIST_RESTRICTION);
			return false;
		}
	}

	if (advertiser_blacklist)
	{
		const Ads_GUID_Set &blacklist = *advertiser_blacklist;
		if (blacklist.find(ad.advertiser_) != blacklist.end())
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE, "%s failed (advertiser %ld, blacklist)\n", ADS_ADVERTISEMENT_LOOPED_ID_CSTR(ad.id_, looped), ads::entity::id(ad.advertiser_)));
			if (is_watched && ad_lite == nullptr)
				ctx->log_watched_info(ad, "RESTRICTION_ADVERTISER_BLACKLIST " + restriction.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_ADVERTISER, ad.advertiser_));
			else if (is_watched && ad_lite != nullptr)
				ctx->log_watched_info(*ad_lite, "RESTRICTION_ADVERTISER_BLACKLIST " + restriction.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_ADVERTISER, ad_lite->ad_->advertiser_));

			ctx->add_restricted_candidate_log(ad.id_, "ADVERTISER_IN_BLACK_LIST");
			ctx->mkpl_executor_->log_ad_error(ctx, ad, ad_owner, selection::Error_Code::ADVERTISER_BLACKLIST_RESTRICTION);
			return false;
		}
	}

	bool have_adv_whitelist = false;
	bool have_brd_whitelist = false;
	const Ads_GUID_Set *adv_whitelist = nullptr;
	const Ads_GUID_Set *brd_whitelist = nullptr;

	if (advertiser_whitelist && !ad.is_external_)
	{
		const auto it = advertiser_whitelist->find(ad.network_id_);
		if (it != advertiser_whitelist->end())
		{
			adv_whitelist = &(it->second);
			have_adv_whitelist = !adv_whitelist->empty();
		}
	}

	if (brand_whitelist && !ad.is_external_)
	{
		const auto it = brand_whitelist->find(ad.network_id_);
		if (it != brand_whitelist->end())
		{
			brd_whitelist = &(it->second);
			have_brd_whitelist = !brd_whitelist->empty();
		}
	}

	if ((have_adv_whitelist || have_brd_whitelist)
		&& !(have_adv_whitelist && adv_whitelist->find(ad.advertiser_) != adv_whitelist->end())
		&& !(have_brd_whitelist && brd_whitelist->find(ad.brand_) != brd_whitelist->end()))
	{
		if (verbose)
			ADS_DEBUG((LP_TRACE, "%s failed (brand %ld & advertiser %ld, whitelist)\n", ADS_ADVERTISEMENT_LOOPED_ID_CSTR(ad.id_, looped), ads::entity::id(ad.brand_), ads::entity::id(ad.advertiser_)));
		if (is_watched && ad_lite == nullptr)
		{
			ctx->log_watched_info(ad, "RESTRICTION_BRAND_WHITELIST " + restriction.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_BRAND, ad.brand_, false /* whitelist */));
			ctx->log_watched_info(ad, "RESTRICTION_ADVERTISER_WHITELIST " + restriction.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_ADVERTISER, ad.advertiser_, false /* whitelist */));
		}
		else if (is_watched && ad_lite != nullptr)
		{
			ctx->log_watched_info(*ad_lite, "RESTRICTION_BRAND_WHITELIST " + restriction.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_BRAND, ad_lite->ad_->brand_, false ));
			ctx->log_watched_info(*ad_lite, "RESTRICTION_ADVERTISER_WHITELIST " + restriction.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_ADVERTISER, ad_lite->ad_->advertiser_, false));
		}
		ctx->add_restricted_candidate_log(ad.id_, "BRAND_ADVERTISER_NOT_IN_WHITE_LIST");
		return false;
	}

	// Pass industry_blacklist as NULL when call this function. Retain this industry_blacklist logic for completeness and refactor.
	/*
	if (industry_blacklist)
	{
		const Ads_GUID_Set & blacklist = *industry_blacklist;

		Ads_GUID_RSet industry_categories;
		Ads_Compliance_Source compliance_source = ad.get_advertiser_categories(industry_categories, slot);
		if (compliance_source.is_external() || (ad_owner.to_mkpl_participant_node() != nullptr && !ad.is_external_))
		{
			if (industry_categories.empty() && !blacklist.empty())
			{
				if (verbose)
					ADS_DEBUG((LP_TRACE, "%s failed (category) without industry in %s slot\n",  ADS_ADVERTISEMENT_LOOPED_ID_CSTR(ad.id_, looped), (slot ? slot->custom_id_.c_str() : "all")));
				if (is_watched && ad_lite == nullptr)
					ctx_->log_watched_info(ad, "RESTRICTION_CATEGORY_BLACKLIST");
				else if (is_watched && ad_lite != nullptr)
					ctx_->log_watched_info(*ad_lite, "RESTRICTION_CATEGORY_BLACKLIST");
				if (compliance_source.is_external())
					ad.log_external_industry_restricted();
				if (ad_lite != nullptr)
					ad_lite->log_selection_error(selection::COMPLIANCE_CHECK_FAILED, slot);

				ctx_->add_restricted_candidate_log(ad.id_, "BLOCKED_BY_INDUSTRY_RESTRICTION");
				ctx_->mkpl_executor_->log_ad_error(ctx_, ad, selection::Error_Code::ADVERTISER_INDUSTRY_RESTRICTION);
				return false;
			}
		}

		for (Ads_GUID_RSet::const_iterator ait = industry_categories.begin();  ait != industry_categories.end(); ++ait)
		{
			if (blacklist.find(*ait) != blacklist.end())
			{
				if (verbose)
					ADS_DEBUG((LP_TRACE, "%s failed (category) with industry %ld in %s slot\n",  ADS_ADVERTISEMENT_LOOPED_ID_CSTR(ad.id_, looped), ads::entity::id(*ait), (slot ? slot->custom_id_.c_str() : "all")));
				if (is_watched && ad_lite == nullptr)
					ctx_->log_watched_info(ad, "RESTRICTION_CATEGORY_BLACKLIST " + restriction.info_.to_string(ctx_, Ads_Asset_Restriction::RESTRICTION_INDUSTRY, *ait));
				else if (is_watched && ad_lite != nullptr)
					ctx_->log_watched_info(*ad_lite, "RESTRICTION_CATEGORY_BLACKLIST " + restriction.info_.to_string(ctx_, Ads_Asset_Restriction::RESTRICTION_INDUSTRY, *ait));
				if (compliance_source.is_external())
					ad.log_external_industry_restricted();
				if (ad_lite != nullptr)
					ad_lite->log_selection_error(selection::COMPLIANCE_CHECK_FAILED, slot);

				ctx_->add_restricted_candidate_log(ad.id_, "BLOCKED_BY_INDUSTRY_RESTRICTION");
				ctx_->mkpl_executor_->log_ad_error(ctx_, ad, selection::Error_Code::ADVERTISER_INDUSTRY_RESTRICTION);
				return false;
			}
		}
	}
	*/
	if (!(ad.is_market_management_placeholder() || ad.is_external_bridge_ad()))
	{
		Ads_GUID network_id = (ad.is_external_) ? ad.external_network_id_ : ad.network_id_;
		if (reseller_blacklist)
		{
			const Ads_GUID_Set &blacklist = *reseller_blacklist;
			if (blacklist.find(network_id) != blacklist.end())
			{
				if (verbose)
					ADS_DEBUG((LP_TRACE, "%s failed (reseller %ld, blacklist)\n", ADS_ADVERTISEMENT_LOOPED_ID_CSTR(ad.id_, looped), ads::entity::id(network_id)));
				if (is_watched && ad_lite == nullptr)
					ctx->log_watched_info(ad, "RESTRICTION_RESELLER_BLACKLIST " + restriction.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_RESELLER, network_id));
				else if (is_watched && ad_lite != nullptr)
					ctx->log_watched_info(*ad_lite, "RESTRICTION_RESELLER_BLACKLIST " + restriction.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_RESELLER, network_id));
				if (ad_lite != nullptr)
					ad_lite->log_selection_error(selection::Error_Code::RESELLER_BLACKLIST_BANNED);
				ctx->add_restricted_candidate_log(ad.id_, "RESELLER_IN_BLACK_LIST");
				return false;
			}
		}

		if (reseller_whitelist)
		{
			const Ads_GUID_Set &whitelist = *reseller_whitelist;
			if (!whitelist.empty() && whitelist.find(network_id) == whitelist.end())
			{
				if (verbose)
					ADS_DEBUG((LP_TRACE, "%s failed (reseller %ld, whitelist)\n", ADS_ADVERTISEMENT_LOOPED_ID_CSTR(ad.id_, looped), ads::entity::id(network_id)));
				if (is_watched && ad_lite == nullptr)
					ctx->log_watched_info(ad, "RESTRICTION_RESELLER_WHITELIST " + restriction.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_RESELLER, network_id, false /* whitelist */));
				else if (is_watched && ad_lite != nullptr)
					ctx->log_watched_info(*ad_lite, "RESTRICTION_RESELLER_WHITELIST " + restriction.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_RESELLER, network_id, false ));
				if (ad_lite != nullptr)
					ad_lite->log_selection_error(selection::Error_Code::RESELLER_WHITELIST_NOT_ALLOWED);
				ctx->add_restricted_candidate_log(ad.id_, "RESELLER_NOT_IN_WHITE_LIST");
				return false;
			}
		}
	}

	if (!(ad.flag_ & Ads_Advertisement::FLAG_INVENTORY_TARGETING_RESTRICTION_EXEMPT) &&
		(ctx->request_->rep()->ads_.check_targeting_ || !ctx->request_->has_explicit_candidates()))
	{
		auto associated_inventory_terms = [&](ads::MEDIA_TYPE assoc) -> const Ads_GUID_RSet& {
			const auto& terms = ad.terms(assoc);
			if (!terms.empty() || ad.placement_ == nullptr)
				return terms;
			return ad.placement_->terms(assoc);
		};

		Ads_GUID owner_nw_id = ad_owner.network_.id_;
		if (targeting_inventory_blacklist && !ad.is_custom_portfolio())
		{
			for (Ads_GUID src_asset_id : *targeting_inventory_blacklist)
			{
				Ads_GUID asset_id = ads::entity::restore_looped_id(src_asset_id);
				bool looped_inventory = ads::entity::is_looped_id(src_asset_id);
				const Ads_Asset_Base* asset = nullptr;
				ctx->find_asset_or_section(asset_id, asset);
				if (!asset || ads::entity::make_looped_id(asset->network_id_, looped_inventory) != ads::entity::make_looped_id(owner_nw_id, looped)) continue;

				ads::MEDIA_TYPE assoc = (ads::entity::type(asset->id_) == ADS_ENTITY_TYPE_ASSET ? ads::MEDIA_VIDEO : ads::MEDIA_SITE_SECTION);
				const Ads_GUID_RSet& terms = associated_inventory_terms(assoc);
				if (terms.find(asset->id_) != terms.end())
				{
					if (verbose)
						ADS_DEBUG((LP_TRACE, "%s failed (ad targeting blacklist) asset_id=%ld\n", ad_lite ? CANDIDATE_LITE_ID_CSTR(ad_lite) : ADS_ADVERTISEMENT_LOOPED_ID_CSTR(ad.id_, looped), ads::entity::id(asset->id_)));
					if (is_watched && ad_lite == nullptr)
						ctx->log_watched_info(ad, "RESTRICTION_TARGETING_INVENTORY_BLACKLIST " + restriction.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_TARGETING, asset->id_));
					else if (is_watched && ad_lite != nullptr)
						ctx->log_watched_info(*ad_lite, "RESTRICTION_TARGETING_INVENTORY_BLACKLIST " + restriction.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_TARGETING, asset->id_));
					ctx->add_restricted_candidate_log(ad.id_, "INVENTORY_TARGETING_IN_BLACK_LIST");
					if (ad_lite != nullptr)
						ad_lite->log_selection_error(selection::Error_Code::AD_TARGETING_RESTRICTED);
					return false;
				}
			}
		}

		if (targeting_inventory_whitelist && !ad.is_custom_portfolio())
		{
			for (Ads_GUID src_asset_id : *targeting_inventory_whitelist)
			{
				Ads_GUID asset_id = ads::entity::restore_looped_id(src_asset_id);
				bool looped_inventory = ads::entity::is_looped_id(src_asset_id);
				const Ads_Asset_Base* asset = nullptr;
				ctx->find_asset_or_section(asset_id, asset);
				if (!asset) continue;

				Ads_GUID inventory_network_id = ads::entity::make_looped_id(asset->network_id_, looped_inventory);
				if(inventory_network_id != ads::entity::make_looped_id(owner_nw_id, looped)) continue;

				ads::MEDIA_TYPE assoc = (ads::entity::type(asset->id_) == ADS_ENTITY_TYPE_ASSET ? ads::MEDIA_VIDEO : ads::MEDIA_SITE_SECTION);
				const Ads_GUID_RSet& terms = associated_inventory_terms(assoc);

				bool found = false;
				for (Ads_GUID ad_asset_id : terms)
				{
					if (ad_asset_id == asset->id_ || ctx->is_parent_asset(ad_asset_id, asset->id_, &ad_owner.network_inventories().assets(assoc)))
					{
						found = true;
						break;
					}
				}

				if (!found)
				{
					if (verbose)
						ADS_DEBUG((LP_TRACE, "%s failed (ad targeting whitelist) asset_id=%ld\n", ad_lite ? CANDIDATE_LITE_ID_CSTR(ad_lite) : ADS_ADVERTISEMENT_LOOPED_ID_CSTR(ad.id_, looped), ads::entity::id(asset->id_)));
					if (is_watched && ad_lite == nullptr)
						ctx->log_watched_info(ad, "RESTRICTION_TARGETING_INVENTORY_WHITELIST " + restriction.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_TARGETING, asset->id_, false /* whitelist */));
					else if (is_watched && ad_lite != nullptr)
						ctx->log_watched_info(*ad_lite, "RESTRICTION_TARGETING_INVENTORY_WHITELIST " + restriction.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_TARGETING, asset->id_, false));
					ctx->add_restricted_candidate_log(ad.id_, "INVENTORY_TARGETING_NOT_IN_WHITE_LIST");
					if (ad_lite != nullptr)
						ad_lite->log_selection_error(selection::Error_Code::AD_TARGETING_RESTRICTED);
					return false;
				}
			}
		}
	}

	return true;
}

bool
Ads_Selector::test_comscore_targeting(Ads_Selection_Context *ctx, const Ads_Advertisement* ad) {
	if (ad == nullptr)
		return false;
	// TODO(pfliu): Consider if this function should be moved to Ads_Advertisement_Candidate_Lite
	if (ad->comscore_terms_.empty())
		return true;
	if (!ctx->root_section_)
		return false;
	// hide comscore index data from reseller
	if (ad->network_id_ != ctx->root_section_->network_id_)
	{
		ADS_LOG((LP_TRACE, "reseller(%s)'s ad(%s) cannot do comscore index targeting on cro(%s)'s section(%s)\n",
			ADS_ENTITY_ID_CSTR(ad->network_id_), ADS_ENTITY_ID_CSTR(ad->id_),
			ADS_ENTITY_ID_CSTR(ctx->root_section_->network_id_), ADS_ENTITY_ID_CSTR(ctx->root_section_->id_)));
		return false;
	}
	if (ctx->comscore_index_.empty())
		return false;
	bool ok = true;
	if (ad->comscore_relation_ == Ads_Advertisement::COMSCORE_RELATION_OR)
	{
		ALOG(LP_TRACE, "comscore targeting of ad %s relation is or\n", ADS_ENTITY_ID_CSTR(ad->id_));
		ok = false;
		for (Ads_Advertisement::Comscore_Vector::const_iterator it = ad->comscore_terms_.begin(); it != ad->comscore_terms_.end(); ++it)
		{
			if (test_comscore_term(ctx, *it))
			{
				ok = true;
				break;
			}
		}
	}
	else
	{
		ALOG(LP_TRACE, "comscore targeting of ad %s relation is and\n", ADS_ENTITY_ID_CSTR(ad->id_));
		ok = true;
		for (Ads_Advertisement::Comscore_Vector::const_iterator it = ad->comscore_terms_.begin(); it != ad->comscore_terms_.end(); ++it)
		{
			if (!test_comscore_term(ctx, *it))
			{
				ok = false;
				break;
			}
		}
	}
	ALOG(LP_TRACE, "comscore targeting of ad(%s) %s!\n", ADS_ENTITY_ID_CSTR(ad->id_), ok ? "succeed" : "failed");
	return ok;
}

bool
Ads_Selector::test_comscore_term(Ads_Selection_Context *ctx, const Ads_Comscore_Term *term)
{
	Ads_Section_Base::Comscore_Index_Map::const_iterator it = ctx->comscore_index_.find(term->demographic_id_);
	if (it == ctx->comscore_index_.end())
	{
		ADS_LOG((LP_ERROR, "demographic index %s not found\n", ADS_ENTITY_ID_CSTR(term->demographic_id_)));
		return false;
	}
	int uv_index = it->second;
	if ((term->maximum_index_ == -1 || uv_index <= term->maximum_index_) && (term->minimum_index_ == -1 || uv_index >= term->minimum_index_))
	{
		ADS_DEBUG((LP_TRACE, "test comscore demo %s, uv %d match Ads_Comscore_Term %s succeed!\n", ADS_ENTITY_ID_CSTR(term->demographic_id_), uv_index, term->to_string().c_str()));
		return true;
	}
	else
	{
		ADS_DEBUG((LP_TRACE, "test comscore demo %s, uv %d match Ads_Comscore_Term %s failed!\n", ADS_ENTITY_ID_CSTR(term->demographic_id_), uv_index, term->to_string().c_str()));
		return false;
	}
}

int
Ads_Selector::collect_targetable_candidates(Ads_Selection_Context* ctx,
                                            Ads_Term_Criteria::ENTITY_TYPE target_type,
                                            const selection::Advertisement_Owner &ad_owner,
                                            const Ads_GUID_Set& terms,
                                            Ads_GUID_Set& candidates,
                                            const Collecting_Targetable_Candidate_Filter &candidate_filter,
                                            const std::set<Ads_Term::TYPE>* specified_term_types)
{
	ADS_DEBUG((LP_DEBUG, "collecting candidate entities: type %s\n", Ads_Term_Criteria::get_entity_type_name(target_type)));
	int ret = collect_targetable_candidates(ctx, target_type, ad_owner.network_, terms, candidates, candidate_filter, specified_term_types);
	ADS_DEBUG((LP_TRACE, "%d candidates type %s collected for %s: %s\n", candidates.size(), Ads_Term_Criteria::get_entity_type_name(target_type), ad_owner.tag_str().c_str(),  ads::entities_to_str(candidates).c_str()));
	return ret;
}

int
Ads_Selector::collect_targetable_candidates(Ads_Selection_Context* ctx,
                                            Ads_Term_Criteria::ENTITY_TYPE target_type,
                                            const Ads_Network& network,
                                            const Ads_GUID_Set& terms,
                                            Ads_GUID_Set& candidates,
                                            const Collecting_Targetable_Candidate_Filter &candidate_filter,
                                            const std::set<Ads_Term::TYPE>* specified_term_types)
{
	// Generally (input parameter) target_type == Ads_Term_Criteria.target_type_, except for the exchange scenario

	typedef std::map<std::pair<int, const Ads_Term_Criteria *>, Criteria_Info> Score_Map;
	const Ads_Repository *repo = ctx->repository_;

	auto calculate_criteria_target = [&specified_term_types](const Ads_Term_Criteria* criteria)->int {
		if (specified_term_types == nullptr || specified_term_types->empty() || criteria->type_ != Ads_Term_Criteria::AND)
		{
			return (criteria->type_ == Ads_Term_Criteria::AND) ? (int)(criteria->children_.size() + criteria->terms_.size()) : 1;
		}

		int target = 0;
		for (const auto *child_criteria : criteria->children_)
		{
			if (!child_criteria)
				continue;

			if (ads::set_is_intersected(*specified_term_types, child_criteria->included_term_types_))
			{
				target++;
			}
		}

		for (Ads_GUID id : criteria->terms_)
		{
			Ads_Term::TYPE type = (Ads_Term::TYPE)ads::term::type(id);
			if (specified_term_types->find(type) != specified_term_types->end())
			{
				target++;
			}
		}
		return target;
	};

	Score_Map scores;
	auto update_scores = [&scores, &calculate_criteria_target](const Ads_Term_Criteria* criteria) {
		auto score_it = scores.find(std::make_pair(criteria->level_, criteria));
		if (score_it == scores.end())
		{
			int target = calculate_criteria_target(criteria);
			scores.emplace(std::make_pair(criteria->level_, criteria), Criteria_Info(1, target, false));
		}
		else if (score_it->second.score_ > 0)
			++(score_it->second.score_);
	};

	//XXX: perf debugging
#if defined(ADS_ENABLE_FORECAST)
	int index = std::max(0, std::min(PERF_COUNTER_CNT - 1, (int)target_type));
	++ctx->perf_counters_[index];
#else
	//Disable perf_counters_ temporarily
	//if (target_type == Ads_Term_Criteria::ADVERTISEMENT) ++ctx->perf_counters_[0];
	//else ++ctx->perf_counters_[1];
#endif

	Ads_GUID network_id = network.id_;
	if (target_type == Ads_Term_Criteria::ADVERTISEMENT)
	{
		std::hash_set<const Ads_Term_Criteria*> content_root_criteria;
		std::vector<Ads_GUID> content_terms;
		std::vector<Ads_GUID> other_terms;
		content_terms.reserve(50);
		other_terms.reserve(50);

		for (Ads_GUID term_id : terms)
		{
			Ads_Term::TYPE type = (Ads_Term::TYPE) ads::term::type(term_id);
			// content_package will be flatten in pusher as asset/asset_group/section/section_group
			// placements which contains CONTENT_PACKAGE(criteria type = 22) and don't contains MEDIA(criteria type = 15) will be filtered when ads don't check Ads_Term::CONTENT_PACKAGE
			if ( type == Ads_Term::ASSET || type == Ads_Term::SERIES || type == Ads_Term::ASSET_GROUP || type == Ads_Term::RON_ASSET_GROUP
				|| type == Ads_Term::SECTION || type == Ads_Term::SITE || type == Ads_Term::SECTION_GROUP || type == Ads_Term::RON_SECTION_GROUP
				|| type == Ads_Term::TV_NETWORK
				|| type == Ads_Term::CONTENT_PACKAGE
				|| type == Ads_Term::MKPL_LISTING
				|| type == Ads_Term::MKPL_ROMN)
				content_terms.push_back(term_id);
			else
				other_terms.push_back(term_id);
		}
		content_root_criteria.clear();


		///step 1a. iterms scoring
		for (Ads_GUID term_id : content_terms)
		{
			const Ads_Term *term = nullptr;
			if (ctx->delta_repository_)
			{
				ctx->delta_repository_->find_term(term_id, term);
				if (term && (!ads::entity::is_valid_id(term->network_id_) || term->network_id_ == network_id))
				{
					std::pair<Ads_Term::Criteria_Vector::const_iterator, Ads_Term::Criteria_Vector::const_iterator> range = term->criterias(network_id, target_type);
					for (Ads_Term::Criteria_Vector::const_iterator it = range.first; it != range.second; ++it)
					{
						const Ads_Term_Criteria *criteria = *it;
						if (criteria)
						{
							update_scores(criteria);
							content_root_criteria.insert(criteria->root_criteria_);
						}
					}
				}
			}

			{
				REPO_FIND_ENTITY_CONTINUE(repo, term, term_id, term)

				if (!term || (ads::entity::is_valid_id(term->network_id_) && term->network_id_ != network_id))
					continue;

				auto range = term->criterias(network_id, target_type);
				for (Ads_Term::Criteria_Vector::const_iterator it = range.first; it != range.second; ++it)
				{
					const Ads_Term_Criteria *criteria = *it;
					if (criteria && criteria->active_)
					{
						update_scores(criteria);
						content_root_criteria.insert(criteria->root_criteria_);
					}
				}
			}
		}

		for (Ads_GUID term_id : other_terms)
		{
			const Ads_Term *term = nullptr;
			if (ctx->delta_repository_)
			{
				ctx->delta_repository_->find_term(term_id, term);
				if (term && (!ads::entity::is_valid_id(term->network_id_) || term->network_id_ == network_id))
				{
					std::pair<Ads_Term::Criteria_Vector::const_iterator, Ads_Term::Criteria_Vector::const_iterator> range = term->criterias(network_id, target_type);
					for (Ads_Term::Criteria_Vector::const_iterator it = range.first; it != range.second; ++it)
					{
						const Ads_Term_Criteria *criteria = *it;
						if (criteria)
						{
							if (criteria->root_criteria_ && criteria->root_criteria_->has_content_targeting_ && content_root_criteria.find(criteria->root_criteria_) == content_root_criteria.end())
								continue;
							update_scores(criteria);
						}
					}
				}
			}

			{
				REPO_FIND_ENTITY_CONTINUE(repo, term, term_id, term)

				if (!term || (ads::entity::is_valid_id(term->network_id_) && term->network_id_ != network_id))
					continue;

				auto range = term->criterias(network_id, target_type);
				for (Ads_Term::Criteria_Vector::const_iterator it = range.first; it != range.second; ++it)
				{
					const Ads_Term_Criteria *criteria = *it;
					if (criteria && criteria->active_)
					{
						if (criteria->root_criteria_ && criteria->root_criteria_->has_content_targeting_ && content_root_criteria.find(criteria->root_criteria_) == content_root_criteria.end())
							continue;
						update_scores(criteria);
					}
				}
			}
		}
	}
	else
	{
		///step 1a. iterms scoring
		for (Ads_GUID term_id : terms)
		{
			const Ads_Term *term = nullptr;
			if (ctx->delta_repository_)
			{
				ctx->delta_repository_->find_term(term_id, term);
				if (term && (!ads::entity::is_valid_id(term->network_id_) || term->network_id_ == network_id))
				{
					std::pair<Ads_Term::Criteria_Vector::const_iterator, Ads_Term::Criteria_Vector::const_iterator> range = term->criterias(network_id, target_type);
					for (Ads_Term::Criteria_Vector::const_iterator it = range.first; it != range.second; ++it)
					{
						const Ads_Term_Criteria *criteria = *it;
						if (criteria)
						{
							update_scores(criteria);
						}
					}
				}
			}

			{
				REPO_FIND_ENTITY_CONTINUE(repo, term, term_id, term)

				if (!term || (ads::entity::is_valid_id(term->network_id_) && term->network_id_ != network_id))
					continue;

				auto range = term->criterias(network_id, target_type);
				for (Ads_Term::Criteria_Vector::const_iterator it = range.first; it != range.second; ++it)
				{
					const Ads_Term_Criteria *criteria = *it;
					if (criteria && criteria->active_)
					{
						update_scores(criteria);
					}
				}
			}
		}
	}

	///step 2: expand and collect
	std::vector<const Ads_Term_Criteria *> succeeded, failed, almost_succeeded;
	succeeded.reserve(scores.size());
	failed.reserve(scores.size());
	almost_succeeded.reserve(scores.size());

	while (true)
	{
		succeeded.clear();
		failed.clear();
		almost_succeeded.clear();
		int current_level = 0xffff;

		for (auto it_scores = scores.begin(); it_scores != scores.end(); ++it_scores)
		{
			const Ads_Term_Criteria * criteria = it_scores->first.second;
			if (!criteria || it_scores->second.processed_)
				continue;

			int score = it_scores->second.score_, target = it_scores->second.target_;

			if (score < 0 || score >= target) // hit this criteria
			{
				bool ok = ((!criteria->negative_ && score > 0)  || (criteria->negative_ && score < 0));
				if (ok)
				{
					// to prevent premature succeeding we allow the lowest level at one iteration for safety
					if (criteria->has_negative_children_)
					{
						if (criteria->level_ < current_level)
						{
							almost_succeeded.clear();
							almost_succeeded.push_back(criteria);
							current_level = criteria->level_;
						}
						else if (criteria->level_ == current_level)
							almost_succeeded.push_back(criteria);
					}
					else
						succeeded.push_back(criteria);
				}
				else
				{
					failed.push_back(criteria);
					it_scores->second.processed_ = true;
				}

				// check whether need to update parent_criteria
				if (ok)
				{
					if (criteria->has_negative_children_)
						continue;
					if (criteria->negative_ && criteria->type_ == Ads_Term_Criteria::AND)
						continue;
				}

				for (const auto *parent_criteria : criteria->parents_)
				{
					if (parent_criteria)
					{
						if (ok)
						{
							/// propagate when it is succeeded
							update_scores(parent_criteria);
						}
						else if (parent_criteria->type_ == Ads_Term_Criteria::AND)
						{
							/// or failed
							scores[{parent_criteria->level_, parent_criteria}].score_ = -1;
						}
					}
				}
			}
		}

		/// escape
		bool update_parents = false;
		if (succeeded.empty() && failed.empty())
		{
			/// propagate almost succeeded ones (of the lowest level) when there is nothing else to propagate
			if (!almost_succeeded.empty())
			{
				succeeded.swap(almost_succeeded);
				update_parents = true;
			}
		}

		if (succeeded.empty() && failed.empty())
			break;

		if (!ctx->watched_criterias_.empty())
		{
			for (const Ads_Term_Criteria *criteria : failed)
			{
				if (ctx->watched_criterias_.find(criteria->id_) != ctx->watched_criterias_.end())
					ctx->watched_criterias_[criteria->id_] = -1;
			}
		}

		for (const Ads_Term_Criteria *criteria : succeeded)
		{
			if (!ctx->watched_criterias_.empty() &&
			    ctx->watched_criterias_.find(criteria->id_) != ctx->watched_criterias_.end())
				ctx->watched_criterias_[criteria->id_] = 1;

			//scores.erase(std::make_pair(criteria->level_, criteria));
			scores[{criteria->level_, criteria}].processed_ = true;

			/// delayed propagation for almost succeeded items
			if (update_parents && (!criteria->negative_ || criteria->type_ == Ads_Term_Criteria::OR))
			{
				for (const auto *parent_criteria : criteria->parents_)
				{
					if (parent_criteria)
					{
						update_scores(parent_criteria);
					}
				}
			}

			if (!criteria->advertisements_.empty())
			{
				ADS_DEBUG((LP_TRACE, "%s, criteria %ld succeed, advertisements size %ld\n",
					ADS_ENTITY_ID_TAG_CSTR(network_id), ads::entity::id(criteria->id_), criteria->advertisements_.size()));
			}

			for (Ads_GUID ad_id : criteria->advertisements_)
			{
				if (candidate_filter)
				{
					ad_id = candidate_filter(ad_id, *criteria);
					if (!ads::entity::is_valid_id(ad_id))
						continue;
				}

				candidates.insert(ad_id);
			}
		}
		ADS_DEBUG0((LP_TRACE, "\n"));
	}
	return 0;
}

int
Ads_Selector::collect_targetable_advertisements(Ads_Selection_Context *ctx, selection::Advertisement_Owner &ad_owner,
						const Ads_GUID_Set& terms, Ads_GUID_Set &candidates, Ads_GUID_Set *banned_candidates)
{
	const auto *closure = ad_owner.to_closure();
	Ads_GUID loopable_network_id = (closure != nullptr ? closure->network_id_ : ad_owner.network_.id_);
	std::unordered_set<Ads_GUID> noncandidates;
	Ads_GUID_Set additional_merged_terms;

	auto candidate_filter = [&](Ads_GUID ad_id, const Ads_Term_Criteria& criteria)->Ads_GUID {
		if (noncandidates.find(ad_id) != noncandidates.end())
			return -1;
		const Ads_Advertisement* ad = nullptr;
		bool is_in_delta_repo = (ctx->find_advertisement(ad_id, ad) == AD_IN_DELTA_REPO);

		// if ad in delta repository, the criteria MUST be also from delta repository
		if (ad == nullptr || is_in_delta_repo != criteria.delta_)
			return -1;

		if (ad->is_portfolio())
		{
			if (Ads_Server_Config::instance()->forecast_record_ron_portfolio_ || ad->is_custom_portfolio())
			ctx->forecast_portfolio_ids_[loopable_network_id].insert(ad_id);
#ifndef ADS_ENABLE_FORECAST
			// skip portfolio candidate (only during ADS selection, while forecast need it)
			return -1;
#endif
		}

		if (ad->network_id_ != ad_owner.network_.id_)
			return -1;

		bool sponsored = false;
		if (!this->test_advertisement(*ctx, *ad, ad_owner, sponsored))
		{
			if (banned_candidates != nullptr && ad->is_active(ctx->time(), ctx->enable_testing_placement()) && !ad->is_external_)
				banned_candidates->insert(ad_id);
			if (sponsored)
			{
				ctx->sponsorship_controller_->invalid_sponsor_ads().insert(ad_id);
				return ads::entity::make_looped_id(ad_id, ads::entity::is_looped_id(loopable_network_id));
			}
			else
			{
				noncandidates.insert(ad_id);
				return -1;
			}
		}

		const Ads_Advertisement_Candidate_Lite *ad_lite = ctx->create_advertisement_candidate_lite(ad, ad_owner, is_in_delta_repo);
		bool ad_test_ok = this->test_advertisement_candidate_lite(*ctx, *ad_lite);
		if (!ad_test_ok && banned_candidates != nullptr && !ad->is_external_)
		{
			banned_candidates->insert(ad_id);
		}

		// Do additional check if the check in test_advertisement and test_advertisement_ad_lite are passed.
		bool additional_targetable = true;
		if (ad_test_ok && !ctx->additional_negative_terms_.empty() && ad_lite->targeting_criteria())
		{
			if (additional_merged_terms.empty())
			{
				additional_merged_terms = ad_owner.terms_;
				additional_merged_terms.insert(ctx->additional_negative_terms_.begin(), ctx->additional_negative_terms_.end());
			}
			additional_targetable = this->test_targeting_criteria(ctx->repository_, ctx->delta_repository_, additional_merged_terms, ad_lite->targeting_criteria());

			if (!additional_targetable)
			{
				if (ctx->is_watched(*ad_lite))
					ctx->log_watched_info(*ad_lite, "ADDITIONAL_NEGATIVE_TARGETING");
				ADS_DEBUG0((LP_TRACE, "ad %s failed (additional negative terms)\n", ADS_ENTITY_ID_CSTR(ad_lite->id())));
			}
		}
		// Sponsored ad will be passed even if the check above are failed. Failed sponsored ad will be add into invalid_sponsor_candidates_ and be filtered in phase8.
		if (!ad_test_ok || !additional_targetable)
		{
			if (sponsored)
				ctx->sponsorship_controller_->invalid_sponsor_ads().insert(ad_id);
			else
			{
				noncandidates.insert(ad_id);
				return -1;
			}
		}

		return ads::entity::make_looped_id(ad_id, ads::entity::is_looped_id(loopable_network_id));
	};

	Ads_Term_Criteria::ENTITY_TYPE target_type = Ads_Term_Criteria::ADVERTISEMENT;
	const mkpl::Participant_Node *node = ad_owner.to_mkpl_participant_node();
	if (node != nullptr && node->inbound_order_ != nullptr && node->inbound_order_->is_exchange_order())
		target_type = Ads_Term_Criteria::ADVERTISEMENT_EXCHANGE;

	return collect_targetable_candidates(ctx, target_type, ad_owner, terms, candidates, candidate_filter);
}

bool
Ads_Selector::test_partial_targeting_criteria(const Ads_Repository *repo, const Ads_Delta_Repository *depo, const Ads_GUID_Set &terms, const Ads_Term_Criteria *criteria, const std::set<Ads_Term::TYPE> *checked_term_types, bool &all_terms_ignored, Ads_GUID_List &path, Ads_GUID_List *failed_path) const
{
	all_terms_ignored = false;
	if (!criteria) return false;

	const size_t MAX_DEPTH = 10;
	if (path.size() >= MAX_DEPTH || std::find(path.begin(), path.end(), criteria->id_) != path.end())
	{
		ADS_ERROR_RETURN((LP_ERROR, "circle detected when evaluating term criteria %s.\n", ADS_ASSET_ID_CSTR(criteria->id_)), false);
	}

	all_terms_ignored = true;
	bool ok = true;
	path.push_back(criteria->id_);

	if (failed_path != NULL)
		failed_path->assign(path.begin(), path.end());

	if (criteria->type_ == Ads_Term_Criteria::OR)
	{
		ok = false;
		for (Ads_GUID_RVector::const_iterator it = criteria->terms_.begin(); it != criteria->terms_.end(); ++it)
		{
			Ads_GUID term_id = *it;
			if (checked_term_types && !is_term_of_types(term_id, *checked_term_types))
				continue;

			ADS_DEBUG((LP_DEBUG, "or relationship criteria check: criteria_id %d, term_id %d, term_type %d\n", ads::entity::id(criteria->id_), term_id, (Ads_Term::TYPE)ads::term::type(term_id)));
			all_terms_ignored = false;
			if (terms.find(*it) == terms.end())
				continue;

			ok = true;
			break;
		}

		if (!ok)
		{
			std::vector<Ads_Term_Criteria*> children(criteria->children_.begin(), criteria->children_.end());
			if (criteria->has_negative_children_)
				std::copy(criteria->negative_children_.begin(), criteria->negative_children_.end(), std::back_inserter(children));

			for (const auto *child : children)
			{
				if (!child)
				{
					all_terms_ignored = false;
					continue;
				}

				bool ignored;
				bool res = test_partial_targeting_criteria(repo, depo, terms, child, checked_term_types, ignored, path, failed_path);
				if (!ignored)
				{
					all_terms_ignored = false;
					if (res)
					{
						ok = true;
						break;
					}
				}
			}
		}
	}
	else // Ads_Term_Criteria::AND
	{
		ok = true;
		for (Ads_GUID_RVector::const_iterator it = criteria->terms_.begin(); it != criteria->terms_.end(); ++it)
		{
			Ads_GUID term_id = *it;
			if (checked_term_types && !is_term_of_types(term_id, *checked_term_types))
				continue;

			ADS_DEBUG((LP_DEBUG, "and relationship criteria check: criteria_id %d, term_id %d, term_type %d\n", ads::entity::id(criteria->id_), term_id, (Ads_Term::TYPE)ads::term::type(term_id)));
			all_terms_ignored = false;
			if (terms.find(term_id) == terms.end())
			{
				ok = false;
				break;
			}
		}

		if (ok)
		{
			std::vector<Ads_Term_Criteria*> children(criteria->children_.begin(), criteria->children_.end());
			if (criteria->has_negative_children_)
				std::copy(criteria->negative_children_.begin(), criteria->negative_children_.end(), std::back_inserter(children));

			for (const auto *child : children)
			{
				if (!child || !ok)
				{
					all_terms_ignored = false;
					break;
				}

				bool ignored;
				bool res = test_partial_targeting_criteria(repo, depo, terms, child, checked_term_types, ignored, path, failed_path);
				if (!ignored)
				{
					all_terms_ignored = false;
					if (!res)
					{
						ok = false;
						break;
					}
				}
			}
		}
	}

	// empty criteria cannot be ignored while no type checking.
	if (!checked_term_types)
	{
		all_terms_ignored = false;
	}

	path.pop_back();
	return (ok && !criteria->negative_) || (!ok && criteria->negative_);
}

bool
Ads_Selector::test_targeting_criteria(const Ads_Repository *repo, const Ads_Delta_Repository *depo, const Ads_GUID_Set &terms, const Ads_Term_Criteria *criteria) const
{
	bool all_terms_ignored = false;
	Ads_GUID_List path;
	bool res = test_partial_targeting_criteria(repo, depo, terms, criteria, nullptr, all_terms_ignored, path, nullptr);
	if (path.size() >= 10)
	{
		Ads_Monitor::stats_inc(Ads_Monitor::TARGETING_DEPTH_OVERFLOW);
	}
	return res;
}

void 
Ads_Selector::collect_listings(Ads_Selection_Context *ctx, Ads_Asset_Section_Closure &closure)
{
	if (ctx->data_right_management_->is_data_right_enabled())
	{
		Ads_GUID_Set available_terms;
		const auto& data_right = ctx->data_right_management_->data_right_of_full_network(closure.network_id_);
		for (Ads_GUID term : closure.terms_)
		{
			if (!ctx->data_right_management_->is_term_restricted(data_right, (Ads_Term::TYPE) ads::term::type(term)))
				available_terms.insert(term);
		}
		collect_targetable_listings(ctx, closure, available_terms);
	}
	else
		collect_targetable_listings(ctx, closure, closure.terms_);
}

void
Ads_Selector::collect_targetable_listings(Ads_Selection_Context *ctx, selection::Advertisement_Owner &ad_owner, const Ads_GUID_Set& available_terms, std::function<bool(const Ads_MKPL_Listing&)> listing_filter)
{
	Ads_Term_Criteria::ENTITY_TYPE target_type = Ads_Term_Criteria::MKPL_LISTING;
	const mkpl::Participant_Node *node = ad_owner.to_mkpl_participant_node();
	if (node != nullptr && node->inbound_order_ != nullptr && node->inbound_order_->is_exchange_order())
		target_type = Ads_Term_Criteria::MKPL_LISTING_EXCHANGE;

	Ads_GUID_Set listing_ids;
	collect_targetable_candidates(ctx, target_type, ad_owner, available_terms, listing_ids);
 
	for (Ads_GUID listing_id : listing_ids)
	{
		auto it = ctx->repository_->mkpl_listings_.find(listing_id);
		if (it != ctx->repository_->mkpl_listings_.end())
		{
			Ads_MKPL_Listing *listing = it->second;
			if (!listing->is_effective(ctx->time()))
				continue;
			if (!listing->inventory_split_sources_.empty() && listing->inventory_split_sources_.find(ad_owner.inventory_split_source_id_) == listing->inventory_split_sources_.end())
				continue;
			if (ctx->request_ && listing->app_advertisement_policy_ != nullptr && ctx->request_->app_advertisement_policy_ != listing->app_advertisement_policy_)
				continue;
			if (listing_filter && !listing_filter(*listing))
				continue;
			ad_owner.listings_.insert(listing);

			auto listing_term_id = ads::term::make_id(Ads_Term::MKPL_LISTING, listing_id);
			ad_owner.terms_.insert(listing_term_id);

			ADS_DEBUG((LP_DEBUG,
				"add listing (term %s) %s for %s\n"
				, ads::term::str(listing_term_id).c_str()
				, ADS_ENTITY_ID_CSTR(listing_id)
				, ad_owner.tag_str().c_str()));
		}
	}
}

void
Ads_Selector::initialize_carriage_routing_slots(Ads_Selection_Context *ctx, selection::Advertisement_Owner &ad_owner)
{
	auto listing_compare = [](const Ads_MKPL_Listing* l, const Ads_MKPL_Listing* r) -> bool {
		bool lempty = l->inventory_split_sources_.empty(), rempty = r->inventory_split_sources_.empty();
		return std::tie(lempty, l->id_) < std::tie(rempty, r->id_);
	};
	std::set<const Ads_MKPL_Listing*, decltype(listing_compare)> split_listings(listing_compare);
	for (const auto *listing : ad_owner.listings_)
	{
		if (!listing->inventory_owners_.empty() || listing->split_units_.empty())
			continue;
		split_listings.insert(listing);
	}

	for (const auto* listing : split_listings)
	{
		const Ads_MKPL_Listing::Split_Unit *split_unit = nullptr;
		for (const auto *su : listing->split_units_)
		{
			if (su->split_ratio_map_.empty() || su->split_percentage_sum_ == 0)
				continue;
			if (this->is_compatible_with_standard_attributes(*su, ctx))
			{
				split_unit = su;
				break;
			}
		}

		if (split_unit == nullptr)
			continue;

		ADS_DEBUG((LP_DEBUG,
				"use split unit %s on carriage listing %s for inventory split\n"
				, ADS_ENTITY_ID_CSTR(split_unit->id_)
				, ADS_ENTITY_ID_CSTR(listing->id_)));

		bool slot_routable = false, slot_routable_on_listing = false;
		for (const auto* acs : ad_owner.all_slot_accesses())
		{
			Ads_Slot_Base *slot = acs->slot();
			Ads_Video_Slot *vslot = dynamic_cast<Ads_Video_Slot*>(slot);
			if (acs->routing().is_valid() ||
				slot->is_tracking_only() ||
				vslot == nullptr ||
				ads::standard_ad_unit(vslot->time_position_class_) == ads::OVERLAY)
			{	
				continue;
			}
			slot_routable = true;

			if (!Ads_MKPL_Listing_Ref::is_listing_compatible_with_slot(*ctx, *listing, *slot))
			{
				continue;
			}
			slot_routable_on_listing = true;

			int seed = ctx->rand() % split_unit->split_percentage_sum_;
			int lower = 0, upper = 0;
			for (const auto& kv : split_unit->split_ratio_map_)
			{
				lower = upper;
				upper += kv.first;
				if (seed >= lower && seed < upper)
				{
					ad_owner.initialize_slot_routing(slot, kv.second, split_unit->id_, listing, *ctx->repository_);
					ADS_DEBUG((LP_DEBUG, "assign inventory owner %s for slot %s, seed %d in range[%d, %d).\n",
								ADS_ENTITY_ID_CSTR(kv.second), vslot->custom_id_.c_str(), seed, lower, upper));
					break;
				}
			}
		}
		
		if (!slot_routable || slot_routable_on_listing)
			break;
	}
}

int
Ads_Selector::assign_cro_closure_slots(Ads_Selection_Context *ctx)
{
	auto add_slot = [&](Ads_Slot_Base *slot, Ads_Asset_Section_Closure *cro_closure) -> void
	{
		Ads_Video_Slot *vslot = dynamic_cast<Ads_Video_Slot*>(slot);
#if !defined(ADS_ENABLE_FORECAST)
		//TODO: We need to further investigate why we skip static_schedule slot here.
		//If we deprecate this logic, it will fail lots of regression cases.
		if (vslot != nullptr && vslot->env() == ads::ENV_VIDEO)
		{
			if (vslot->is_schedule_mode(Ads_Slot_Template::Slot_Info::STATIC_SCHEDULE))
				return;
		}
#endif
		auto &slot_ref = cro_closure->add_slot(slot);
		slot_ref.guaranteed_ = 1;
		if (vslot != nullptr && ads::entity::is_valid_id(vslot->carriage_inventory_owner_))
		{
			cro_closure->initialize_slot_routing(slot, vslot->carriage_inventory_owner_, -1, nullptr, *ctx->repository_);
		}
	};

	if (ctx->root_asset_)
	{
		auto *cro_closure = ctx->closure(ctx->root_asset_->network_id_, false);
		if (cro_closure != nullptr)
		{
			std::for_each(ctx->video_slots_.begin(), ctx->video_slots_.end(), std::bind(add_slot, std::placeholders::_1, cro_closure));
			std::for_each(ctx->player_slots_.begin(), ctx->player_slots_.end(), std::bind(add_slot, std::placeholders::_1, cro_closure));
		}
		//special handling for tracking slot
		if (ctx->tracking_slot_)
		{
			auto &slot_ref = cro_closure->add_slot(ctx->tracking_slot_);
			slot_ref.guaranteed_ = 1;
		}
		initialize_carriage_routing_slots(ctx, *cro_closure);
	}

	if (ctx->root_section_)
	{
		auto *cro_closure = ctx->closure(ctx->root_section_->network_id_, false);
		if (cro_closure != nullptr)
		{
			std::for_each(ctx->page_slots_.begin(), ctx->page_slots_.end(), std::bind(add_slot, std::placeholders::_1, cro_closure));
		}
	}

	return 0;
}

int
Ads_Selector::initialize_ab_test_buckets(Ads_Selection_Context *ctx)
{
	ADS_ASSERT(ctx && ctx->repository_);
	const Ads_Repository* repository = ctx->repository_;

	// Get collection ids targeting all traffic
	Ads_GUID_Set candidates(repository->absolute_ab_test_collection_ids_.begin(),
				repository->absolute_ab_test_collection_ids_.end());

	// Extract asset's closure
	if (ctx->root_asset_)
	{
		Ads_Asset_Section_Closure_Map::const_iterator itr = ctx->closures_.find(ctx->root_asset_->network_id_);
		if (itr != ctx->closures_.end())
		{
			// Get criteria IDs by asset's closure
			collect_targetable_candidates(ctx, Ads_Term_Criteria::AB_TEST, *itr->second, candidates);
		}
	}
	if (ctx->root_section_ &&
		candidates.size() == 0 &&
		(!ctx->root_asset_ || (ctx->root_asset_ && ctx->root_section_->network_id_ != ctx->root_asset_->network_id_))) // CPPCHECK_IGNORE
	{
		// Extract section's closure
		Ads_Asset_Section_Closure_Map::const_iterator itr = ctx->closures_.find(ctx->root_section_->network_id_);
		if (itr != ctx->closures_.end())
		{
			// Get criteria IDs by section's closure
			collect_targetable_candidates(ctx, Ads_Term_Criteria::AB_TEST, *itr->second, candidates);
		}
	}

	// Inject all qualified collections out of criteria set;
	std::list<const Ads_Ab_Test_Collection * > targeted_ab_test_collections;
	std::for_each(candidates.begin(), candidates.end(),
		[repository, &targeted_ab_test_collections](Ads_GUID collection_id)
	{
		auto collection_itr = repository->ab_test_collections_.find(collection_id);
		if (collection_itr != repository->ab_test_collections_.end())
		{
			targeted_ab_test_collections.push_back((*collection_itr).second);
		}
	});

	const int bucket_capacity = Ads_Ab_Test_Bucket::bucket_capacity();    // In OLTP store percentage as format 30(30%, 0.3), transfer it by multiple 10, thus bucket capacity is [0, 1000]
	static const Ads_GUID empty_bucket_id = -1;
	for (const auto *db_collection : targeted_ab_test_collections)
	{
		Ads_GUID active_bucket_id = empty_bucket_id;
		Ads_GUID previous_bucket_id = empty_bucket_id;
		int current_bucket_percentage = -1;
		int previous_bucket_percentage = 0;

		bool cookie_exist = false;

		// Cookie stored previous buckets' information, OLTP stored current buckets' information
		// Find previous buckets and corresponding percentage from cookie
		auto cookie_collection_itr = ctx->user_->ab_test_cookies_map_.find(db_collection->id_);
		const std::map<Ads_GUID, Ab_Test_Bucket> *cookies_buckets = nullptr;
		if (cookie_collection_itr != ctx->user_->ab_test_cookies_map_.end())
			cookies_buckets = &cookie_collection_itr->second;

		int previous_a_bucket_percent = bucket_capacity;
		if (cookies_buckets != nullptr)
		{
			cookie_exist = true;
			for (const auto& cookie_bucket_pair : *cookies_buckets)
			{
				const auto &cookie_bucket = cookie_bucket_pair.second;
				previous_a_bucket_percent -= cookie_bucket.percentage_;
				// Extract active bucket
				if (cookie_bucket.active_)
				{
					previous_bucket_id = cookie_bucket.id_;
					previous_bucket_percentage = cookie_bucket.percentage_;
					// Since we can get collection information against bucket ID in OLTP, thus didn't log collection ID;
					ADS_DEBUG((LP_DEBUG, "AB test info: previous bucket id is %d, previous bucket percentage is %d\n", previous_bucket_id, previous_bucket_percentage));

					auto current_bucket_itr = db_collection->buckets_.find(cookie_bucket.id_);
					// Bucket in cookie didn't exist in current collection
					if (current_bucket_itr == db_collection->buckets_.end())
						continue;
					const auto *current_bucket = current_bucket_itr->second;
					current_bucket_percentage = current_bucket->percentage_;
					ADS_DEBUG((LP_DEBUG, "AB test info: current bucket percentage is %d\n", current_bucket_percentage));
				}
			}
		}

		// Current user is a new user, no collection information previously or invalid data
		if (!cookie_exist || previous_a_bucket_percent < 0 ||
				(previous_bucket_percentage == 0 && (previous_bucket_id != empty_bucket_id || previous_a_bucket_percent == 0)))
		{
			int rand_num = ctx->rand() % bucket_capacity;
			int accumulation = 0;
			for (const auto& db_bucket_iter : db_collection->buckets_)
			{
				Ads_GUID db_bucket_id = db_bucket_iter.first;
				const auto *db_bucket = db_bucket_iter.second;
				accumulation += db_bucket->percentage_;
				if (rand_num < accumulation)
				{
					active_bucket_id = db_bucket_id;
					ADS_DEBUG((LP_DEBUG, "AB test info: user is assigned a new bucket %d\n", active_bucket_id));
					break;
				}
			}
		}
		// Current user already in one bucket, and the bucket's traffic doesn't decrease: stay in this bucket
		else if (current_bucket_percentage >= previous_bucket_percentage)
		{
			active_bucket_id = previous_bucket_id;
		}
		else
		{
			// Current user already in one bucket, but the bucket's traffic decreased: may stay in this bucket,
			// or jump to an increased bucket
			// or jump to A bucket
			if (previous_bucket_id != empty_bucket_id)
			{
				double traffic_dec_rand = (double(ctx->rand() % RAND_MAX) / RAND_MAX) * 100.0;
				if (previous_bucket_percentage > 0 && (double(current_bucket_percentage) / double(previous_bucket_percentage)) * 100.0 >= traffic_dec_rand)
				{
					active_bucket_id = previous_bucket_id;
					ADS_DEBUG((LP_DEBUG, "AB test info: still used previous bucket %d\n", active_bucket_id));
				}
			}
			// Not stay in previous bucket,
			// sample a new bucket by bucket accumulation: either jump to an increased bucket or to A bucket;
			if (active_bucket_id == empty_bucket_id)
			{
				int bucket_accumulation = 0;
				std::map<Ads_GUID, int> bucket_accumulation_map;
				int accumulation = 0;
				// Calculate B bucket's accumulation: only collect from increased buckets
				for (const auto& db_bucket_iter : db_collection->buckets_)
				{
					const auto *db_bucket = db_bucket_iter.second;
					accumulation += db_bucket->percentage_;

					uint32_t previous_percentage = 0;
					if (cookies_buckets != nullptr)
					{
						auto cookie_bucket_iter = cookies_buckets->find(db_bucket->id_);
						if (cookie_bucket_iter != cookies_buckets->end())
							previous_percentage = (*cookie_bucket_iter).second.percentage_;
					}

					if (previous_percentage < db_bucket->percentage_)
					{
						bucket_accumulation_map.insert(std::make_pair(db_bucket->id_, (db_bucket->percentage_ - previous_percentage)));
						bucket_accumulation += (db_bucket->percentage_ - previous_percentage);
					}
				}
				// Insert A bucket's accumulation;
				bucket_accumulation_map.insert(std::make_pair(empty_bucket_id, bucket_capacity - accumulation));
				bucket_accumulation += bucket_capacity - accumulation;

				if (bucket_accumulation > 0)
				{
					int traffic_inc_rand = ctx->rand() % bucket_accumulation;
					accumulation = 0;
					for (const auto& bucket : bucket_accumulation_map)
					{
						accumulation += bucket.second;
						if (traffic_inc_rand < accumulation)
						{
							active_bucket_id = bucket.first;
							ADS_DEBUG((LP_DEBUG, "AB test info: select bucket %d\n", active_bucket_id));
							break;
						}
					}
				}
			}
		}
		// Set active bucket;
		auto db_bucket_itr = db_collection->buckets_.find(active_bucket_id);
		// Target in an active bucket;
		if (db_bucket_itr != db_collection->buckets_.end())
		{
			ctx->ab_test_active_buckets_.emplace(db_collection, db_bucket_itr->second);
		}
		// Target in A bucket;
		else
		{
			ctx->ab_test_active_buckets_.emplace(db_collection, nullptr);
		}
	}
	return 0;
}

bool
Ads_Selector::is_term_of_types(Ads_GUID term_id, const std::set<Ads_Term::TYPE> &types)
{
	Ads_Term::TYPE type = (Ads_Term::TYPE)ads::term::type(term_id);
	if (types.find(type) == types.end())
	{
    	Ads_Term::TYPE category = Ads_Term::category(type);
    	if(category == type || types.find(category) == types.end())
    		return false;
	}

	return true;
}

int
Ads_Selector::flatten_candidate_advertisements(Ads_Selection_Context *ctx, const Ads_GUID_Set& candidates, selection::Advertisement_Owner &ad_owner,
						Ads_Advertisement_Candidate_Lite_Set& flattened_candidates, Ads_GUID_Set *banned_candidates)
{
	ADS_ASSERT(ctx && ctx->repository_);

	Ads_GUID_Set noncandidates;
#if defined(ADS_ENABLE_FORECAST)
    size_t unconstrained_flags = Ads_Request::Smart_Rep::BYPASS_BUDGET_CHECK
        | Ads_Request::Smart_Rep::BYPASS_RESTRICTION
        | Ads_Request::Smart_Rep::FORCE_PROPOSAL
        | Ads_Request::Smart_Rep::SKIP_GENERATE_RESPONSE;
    bool is_unconstrained = (ctx->request_->rep()->flags_ & unconstrained_flags) == unconstrained_flags;
#endif

	for (Ads_GUID ad_id : candidates)
	{
		bool looped = ads::entity::is_looped_id(ad_id);

		const Ads_Advertisement *ad = nullptr;
		if (ctx->find_advertisement(ad_id, ad) < 0 || ad == nullptr) continue;

		const Ads_GUID_RVector& dep = ad->target_dependencies_;

		bool satisfied = true;
		Ads_GUID_RVector::const_iterator end = dep.end();
		for (Ads_GUID_RVector::const_iterator dit = dep.begin(); dit != end; ++dit)
		{
			Ads_GUID dep_ad_id = ads::entity::make_looped_id(*dit, looped);
			if (candidates.find(dep_ad_id) == candidates.end()
			        || noncandidates.find(dep_ad_id) != noncandidates.end())
			{
				satisfied = false;
				break;
			}
		}
#if defined(ADS_ENABLE_FORECAST)
        if (!is_unconstrained && ad->is_portfolio()) satisfied = false;
#endif
		if (!satisfied)
			noncandidates.insert(ad_id);
	}

	Ads_GUID_Set tmp;
	std::set_difference(candidates.begin(), candidates.end(), noncandidates.begin(), noncandidates.end(),
	                    std::insert_iterator<Ads_GUID_Set>(tmp, tmp.begin()));

	while (true)
	{
		Ads_GUID_Set new_candidates;
		for (auto it = tmp.begin(); it != tmp.end(); ++it)
		{
			Ads_GUID ad_id = *it;
			bool looped = ads::entity::is_looped_id(*it);

			Ads_Advertisement_Candidate_Lite *ad_lite = ctx->find_advertisement_candidate_lite(ad_id, &ad_owner);
			if (!ad_lite) continue;

			if (ad_lite->type() == Ads_Advertisement::AD_UNIT)
			{
				if (flattened_candidates.find(ad_lite) != flattened_candidates.end()) continue;

				//if (! ad->targeting_criteria_)
				{
					bool sponsored = false;
					bool ok = this->test_candidate_advertisement(*ctx, *ad_lite, sponsored);

					if (!ok)
					{
						if (banned_candidates != nullptr && ad_lite->is_active(ctx->time(), ctx->enable_testing_placement()) && !ad_lite->is_external())
							banned_candidates->insert(ad_id);
						if (sponsored) ctx->sponsorship_controller_->invalid_sponsor_ads().insert(ad_lite->id());
						else  continue;
					}

					if (ad_lite->is_bumper())
					{
						try_merge_bumper(ctx, ad_lite);
						continue;
					}
				}

				flattened_candidates.insert(ad_lite);
				continue;
			}

			for (Ads_GUID_RSet::const_iterator it = ad_lite->children().begin(); it != ad_lite->children().end(); ++it)
			{
				Ads_GUID child_id = ads::entity::make_looped_id(*it, looped);

				// skip those who are already in the candidate list
				if (tmp.find(child_id) != tmp.end()) continue;

				const Ads_Advertisement_Candidate_Lite *child_lite =
					ctx->find_advertisement_candidate_lite(*it, &ad_lite->owner_);
				if (!child_lite) continue;

				// skip those who have targeting criteria but not in the candidate list
				if (child_lite->targeting_criteria() != ad_lite->targeting_criteria()) continue;

				new_candidates.insert(child_id);
			}
		}

		if (new_candidates.empty()) break;

		tmp.swap(new_candidates);
	}

	return 0;
}

int
Ads_Selector::test_creative_for_request(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate_Lite* lite, Creative_Rendition_List& renditions)
{
	Creative_Rendition_Set applicable_creatives;
	Ads_GUID selected_creative_id = ads::entity::invalid_id(ADS_ENTITY_TYPE_CREATIVE);
	if (lite->ad()->rotation_type_ == Ads_Advertisement::ADVANCED_FLIGHTING_AND_WEIGHTING)
	{
		filter_an_applicable_creative_instance_from_creative_flight_date_set(ctx, lite->ad()->creative_flighting_maps_, selected_creative_id);
		if (!ads::entity::is_valid_id(selected_creative_id, ADS_ENTITY_TYPE_CREATIVE))
		{
			ALOG(LP_DEBUG, "no applicable creative selected for ADVANCED_FLIGHTING_AND_WEIGHTING Advertisement\n");
			return 0;
		}
	}
	for (auto it = lite->ad()->creatives_.begin(); it != lite->ad()->creatives_.end(); ++it)
	{
		const Ads_Creative_Rendition *creative = it->first;
		if (lite->ad()->rotation_type_ == Ads_Advertisement::ADVANCED_FLIGHTING_AND_WEIGHTING)
		{
			// ONLY consider creative who is from flight date set as valid candidate id
			// when AD_UNIT rotation type is ADVANCED_FLIGHTING_AND_WEIGHTING
			if (creative->creative_id_ != selected_creative_id)
			{
				continue;
			}
		}
		if (!this->is_creative_applicable_for_request(ctx, lite, creative, it->second))
		{
			continue;
		}

		renditions.push_back(creative);
		if (creative->creative_ != nullptr)
			applicable_creatives.insert(creative->creative_);
	}

	lite->filter_jit_candidate_creatives(nullptr, applicable_creatives);

	return 0;
}

int
Ads_Selector::filter_an_applicable_creative_instance_from_creative_flight_date_set(Ads_Selection_Context* ctx, const Ads_Advertisement::Creative_Flighting_Map& creative_flighting_maps, Ads_GUID& selected_creative_id)
{
	Ads_Advertisement::Creative_Flighting_Map::const_iterator mit = creative_flighting_maps.begin();
	for (; mit != creative_flighting_maps.end(); ++mit)
	{
		// because different set will not overlap with each other,
		// so, when find out a valid set, then we can break out of this for loop
		if (ctx->in_date_range(mit->second.start_date_, mit->second.end_date_))
		{
			break;
		}
	}
	if (mit == creative_flighting_maps.end())
	{
		ADS_DEBUG((LP_DEBUG, "no applicable set for creative set scheduling\n"));
		return -1;
	}
	auto& candidate_creative_instance_list = mit->second.creative_instances_;
	size_t seed = ctx->rand();
	select_creative_in_flight_date_by_weighting(candidate_creative_instance_list, seed, selected_creative_id);
	return 0;
}

bool
Ads_Selector::apply_effective_curve(const Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate_Lite *ad, double life_stage, int64_t& eCPM, Ads_GUID& curve_id)
{
	auto net_eCPM_saved = eCPM;
	const auto &ad_owner = ad->operation_owner();
	if (ads::entity::is_valid_id(ad_owner.ad_delivery_price_override_id_))
	{
		const Ads_Control_Curve *curve = 0;
	    	///FIXME: M of eCPM mess
		if (ctx->repository_->find_curve(ad_owner.ad_delivery_price_override_id_, curve) >= 0 && curve)
		{
			curve_id = curve->id_;

			eCPM = ADS_TO_MAGNIFIED_CURRENCY(curve->apply(ADS_TO_REAL_CURRENCY(eCPM) * 1000)) / 1000;
			ALOG(LP_DEBUG, "ad: %s, eCPM: %lld -> mapped eCPM: %lld\n", CANDIDATE_LITE_ID_CSTR(ad), net_eCPM_saved, eCPM);
			return true;
		}
	}
	else if (ads::entity::is_valid_id(ad_owner.ad_delivery_price_override_group_id_))
	{
		const Ads_Control_Curve_Group *cg = 0;
		if (ctx->repository_->find_curve_group(ad_owner.ad_delivery_price_override_group_id_, cg) >= 0 && cg)
		{
			curve_id = cg->id_;

			eCPM = ADS_TO_MAGNIFIED_CURRENCY(cg->apply(life_stage, ADS_TO_REAL_CURRENCY(eCPM) * 1000)) / 1000;
			ALOG(LP_DEBUG, "ad: %s, eCPM: %lld -> mapped eCPM: %lld(curve group)\n", CANDIDATE_LITE_ID_CSTR(ad), net_eCPM_saved, eCPM);
			return true;
		}
	}

	return false;
}

int64_t
Ads_Selector::calculate_effective_eCPM(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate &candidate, const Ads_Slot_Base *slot,
					const selection::Advertisement_Owner *host_owner/*null means CRO*/, selection::Effective_ECPM_Factors *factors)
{
	const Ads_Advertisement_Candidate::Slot_Ref *slot_ref = nullptr;
	if (slot == nullptr)
	{
		if (!candidate.slot_references_.empty())
			slot_ref = candidate.slot_references_.begin()->second.get();
	}
	else
	{
		slot_ref = candidate.slot_ref(slot);
	}
	if (slot_ref == nullptr)
		return -1;

	selection::Effective_ECPM_Factors _factors;
	if (factors == nullptr)
		factors = &_factors;

	const auto &net_eCPMs = slot_ref->net_eCPMs_;
	const Ads_MRM_Rule *external_rule = slot_ref->external_rule_;
	Ads_GUID external_rule_ext_id = slot_ref->external_rule_ext_id_;
	const auto &access_path = slot_ref->access_path_;

	const Ads_Advertisement_Candidate_Lite *ad = candidate.lite_;
	double fill_rate = (ad->daily_fill_rate_ >= 0 && ad->daily_fill_rate_ < 1.0) ?
				ad->daily_fill_rate_ : ad->fill_rate_;

	const Ads_Asset_Section_Closure *host_closure = nullptr;
	const mkpl::Participant_Node *host_mkpl_node = nullptr;
	const auto& operation_owner = ad->operation_owner();

	if (host_owner == &operation_owner ||
		(host_owner == nullptr && operation_owner.is_content_right_owner(slot)))
	{
		double life_stage = ad->time_;
		const mkpl::Order_Candidate *outbound_order = nullptr;
		if (external_rule != nullptr)
		{
			factors->rule_boost_ = this->calculate_mrm_rule_boost_factor(ctx, external_rule->id_, external_rule_ext_id, factors->rule_fill_rate_);
			factors->rule_id_ = external_rule->id_;
			factors->rule_ext_id_ = external_rule_ext_id;

			const auto &fill_rate_info = ctx->mrm_rule_fill_rate(*external_rule, external_rule_ext_id);
			life_stage = fill_rate_info.time_;
		}
		else if (access_path.mkpl_path_ != nullptr && &access_path.mkpl_path_->direct_seller().to_ad_owner() == &operation_owner)
		{
			outbound_order = access_path.mkpl_path_->final_buyer().inbound_order_;
			ADS_ASSERT(&outbound_order->seller_node_.to_ad_owner() == &operation_owner);
			life_stage = outbound_order->fill_rate_.time_;
		}

		if (ad->override_eCPM() >= 0)
		{
			factors->base_eCPM_ = ad->override_eCPM();
		}
		else if ((access_path.mkpl_path_ == nullptr && ads::entity::is_valid_id(ad->external_network_id()))
				|| (outbound_order != nullptr && outbound_order->seller_node_.is_root_seller()))
		{ // pure rule or pure rule + order based deal
			factors->base_eCPM_ = net_eCPMs.front().bidding_eCPM_;
		}
		else
		{ // use ecpm from host owner
			factors->base_eCPM_ = (outbound_order != nullptr && net_eCPMs.size() > 1) ?
					std::next(net_eCPMs.rbegin())->bidding_eCPM_ : net_eCPMs.back().bidding_eCPM_;
		}
		ADS_DEBUG((LP_TRACE, "Base ecpm for ad %s is %d\n", CANDIDATE_LITE_ID_CSTR(ad), factors->base_eCPM_));
		/// override the price
		if (ad->guarantee_mode() != Ads_Advertisement::PREEMPTIBLE || candidate.is_guaranteed_deal_ad())
		{
			double effective_fill_rate = fill_rate;
			if (candidate.is_guaranteed_deal_ad())
			{
				if (outbound_order != nullptr)
				{
					effective_fill_rate = outbound_order->fill_rate_.fill_rate_;
					ADS_DEBUG((LP_TRACE, "Order's Fill rate (for PG/BG placeholder ad %s) is %f\n", CANDIDATE_LITE_ID_CSTR(ad), outbound_order->fill_rate_.fill_rate_));
				}
				else
				{
					ADS_ASSERT(external_rule != nullptr);
					effective_fill_rate = factors->rule_fill_rate_;
				}
			}
			apply_effective_curve(ctx, ad, life_stage, factors->base_eCPM_, factors->price_curve_id_);
			factors->ad_boost_ = this->calculate_advertisement_boost_factor(ctx, ad, life_stage, ctx->time(), effective_fill_rate);
		}

		factors->effective_boost_ = (factors->ad_boost_ > 0 ? factors->ad_boost_ : 1.0);
		//use original ecpm for Soft Guaranteed ad
		if (candidate.soft_guaranteed_version_ == Ads_Advertisement_Candidate::SOFT_GUARANTEED_VERSION::V1)
		{
			factors->base_eCPM_ = ad->eCPM(false);
			factors->effective_boost_ = 1.0;
		}
		if (factors->rule_boost_ > 0 && !candidate.is_guaranteed_deal_ad())
			factors->effective_boost_ = factors->rule_boost_;
	}
	else if (access_path.mrm_rule_path_ != nullptr &&
		(host_owner == nullptr || (host_closure = host_owner->to_closure()) != nullptr))
	{
		size_t distance = 0;
		const Ads_MRM_Rule_Path::Edge* host_edge = nullptr;
		for (const auto& edge : access_path.mrm_rule_path_->edges_)
		{
			if (host_closure == nullptr || edge.network_id() == host_closure->network_id_)
			{
				host_edge = &edge;
				break;
			}
			distance++;
		}

		if (host_edge == nullptr)
			return -1;

		auto ecpm_it = std::next(net_eCPMs.begin(), distance);
		ADS_ASSERT(ecpm_it != net_eCPMs.end());
		factors->base_eCPM_ = ecpm_it->bidding_eCPM_;
		factors->effective_boost_ = this->calculate_mrm_rule_boost_factor(ctx, host_edge->rule_id_, host_edge->rule_ext_id_, factors->rule_fill_rate_);
		factors->rule_id_ = host_edge->rule_id_;
		factors->rule_ext_id_ = host_edge->rule_ext_id_;
	}
	else if (access_path.mkpl_path_ != nullptr &&
		(host_owner == nullptr || (host_mkpl_node = host_owner->to_mkpl_participant_node()) != nullptr))
	{
		size_t distance = 0;
		if (dynamic_cast<const mkpl::Secondary_Node*>(host_mkpl_node) != nullptr)
		{
			for (const auto *secondary_node : access_path.mkpl_path_->secondary_nodes_)
			{
				if (secondary_node == host_mkpl_node)
					break;
				distance++;
			}

			if (distance >= access_path.mkpl_path_->secondary_nodes_.size())
				return -1;
			distance += access_path.mkpl_path_->execution_nodes_.size();
		}
		else if (host_mkpl_node != nullptr)
		{
			for (auto it = access_path.mkpl_path_->execution_nodes_.rbegin();
				it != access_path.mkpl_path_->execution_nodes_.rend(); ++it)
			{
				if (*it == host_mkpl_node)
					break;
				distance++;
			}

			if (distance >= access_path.mkpl_path_->execution_nodes_.size())
				return -1;
			distance = access_path.mkpl_path_->execution_nodes_.size() - distance - 1;
		}

		distance += access_path.mrm_rule_path_ ? access_path.mrm_rule_path_->edges_.size() : 0;

		auto ecpm_it = std::next(net_eCPMs.begin(), distance);
		ADS_ASSERT(ecpm_it != net_eCPMs.end());
		factors->base_eCPM_ = ecpm_it->bidding_eCPM_;
		factors->effective_boost_ = 1.0;
	}
	else
	{
		return -1;
	}

	return factors->base_eCPM_ * factors->effective_boost_;
}

int
Ads_Selector::find_best_path_for_advertisement(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate_Lite *ad, Ads_Advertisement_Candidate *&candidate)
{
	ADS_ASSERT(ctx && ctx->repository_ && ctx->request_);
	const Ads_Slot_List &applicable_slots = ad->applicable_slots_;

	if (applicable_slots.empty())
	{
		ad->log_selection_error(selection::Error_Code::NO_APPLICABLE_SLOT);
		ADS_DEBUG((LP_TRACE, "no suitable slot path for network %s:ad %s\n", ADS_ENTITY_ID_CSTR(ad->network_id()), CANDIDATE_LITE_ID_CSTR(ad)));
		return -1;
	}

#if defined(ADS_ENABLE_FORECAST)
	if (ad->is_portfolio())
	{
		Ads_Asset_Section_Closure *closure = ad->owner_.to_closure();
		if (!closure)
		{
			ADS_DEBUG((LP_TRACE, "no connected network %s for ad %s\n", ADS_ENTITY_ID_CSTR(ad->network_id()), ADS_ENTITY_ID_CSTR(ad->id())));
			return -1;
		}
		candidate = new Ads_Advertisement_Candidate(ads::MEDIA_NONE, ad);
		bool found = false;
		for (Ads_Slot_Base* slot : applicable_slots)
		{
			const Ads_MRM_Rule_Path *best_path = nullptr;
			// filter out non restricted ads if all pathes are restricted
			if (this->find_best_path_for_advertisement(ctx, closure, slot, candidate, best_path) < 0)
			{
				ADS_DEBUG0((LP_TRACE, "Custom Portfolio Ad_Unit %d Slot: %s, filter out due to restriction\n", ad->ad_unit()->id_, slot->custom_id_.c_str()));
				continue;
			}
			found = true;
			candidate->assoc_ = (ads::MEDIA_TYPE)(candidate->assoc_ | slot->associate_type());
			candidate->net_eCPMs(slot);
			this->update_advertisement_candidate_priority(ctx, candidate, slot);
			if (best_path != nullptr)
				candidate->access_path(slot).mrm_rule_path_ = best_path;
		}
		if (found)
		{
			return 0;
		}
		else
		{
			delete candidate;
			candidate = 0;
		}
	}
#endif
	Creative_Rendition_List creatives;
	if (ctx->renditions_.empty()) //HOOK: creative rating for non-preview
	{
		test_creative_for_request(ctx, ad, creatives);
		if (creatives.empty() && !ad->is_tracking())
		{
			ADS_DEBUG((LP_TRACE, "no applicable creative for ad %s\n", CANDIDATE_LITE_ID_CSTR(ad)));
			if (ad->is_watched()) ctx->log_watched_info(*ad, nullptr, "NO_CREATIVE");
			ctx->decision_info_.reject_ads_.put(*ad, Reject_Ads::NO_CREATIVES);

#if !defined(ADS_ENABLE_FORECAST)
			ad->log_selection_error(selection::Error_Code::NO_APPLICABLE_CREATIVE);
			return -1;
#endif
		}
	}

#if !defined(ADS_ENABLE_FORECAST)
	if (ad->is_market_management_placeholder())
	{
		if(!ad->precheck_creative_duration_for_market_ad(creatives))
		{
			ad->log_selection_error(selection::Error_Code::AUCTION_MAX_AD_DURATION_EXCEEDED);
			if (ad->is_watched())
				ctx->log_watched_info(*ad, "AUCTION_MAX_AD_DURATION_EXCEEDED");
			return -1;
		}
	}
#endif

	const mkpl::Execution_Path *mkpl_path = nullptr;
	if (!ctx->mkpl_executor_->precheck_advertisement_candidate(ctx, *ad, creatives, mkpl_path))
		return -1;

	candidate = new Ads_Advertisement_Candidate(ads::MEDIA_NONE, ad);
	candidate->init(ctx->repository_, creatives);

	bool found = false;
	double rule_fill_rate = -1.0;
	const int64_t raw_eCPM = ctx->counter_service_->raw_eCPM(ad->cast_to_ad());

	int64_t own_effective_eCPM = -1;
	selection::Effective_ECPM_Factors own_eCPM_factors;

	for (Ads_Slot_Base *slot : applicable_slots)
	{
		int64_t max_eCPM = -1;
		Ads_Inventory_Access_Path max_path;
		std::list<Ads_Advertisement_Candidate::Net_ECPM_Info> net_eCPMs;
		const Ads_MRM_Rule *external_rule = nullptr;
		Ads_GUID external_rule_ext_id = -1;

		const Ads_Asset_Base *root_asset = (slot->associate_type() == ads::MEDIA_SITE_SECTION) ? ctx->root_section_: ctx->root_asset_;
		ADS_ASSERT(root_asset != nullptr);
		if (root_asset == nullptr) continue;

		bool check_price = ((slot->env() != ads::ENV_PAGE && slot->env()  != ads::ENV_PLAYER) || !ad->is_companion());
		int64_t current_eCPM = raw_eCPM;
		const int64_t original_eCPM = current_eCPM;

		if (ad->is_external() && !ad->is_mkpl_programmatic_placeholder())
		{
			Ads_GUID external_network_id = ad->external_network_id();
			if (!ad->is_market_management_placeholder())
			{
				// reseller blacklist
				if (ctx->restriction_.reseller_blacklist_.find(external_network_id) != ctx->restriction_.reseller_blacklist_.end())
				{
					ad->log_selection_error(selection::Error_Code::RESELLER_BLACKLIST_BANNED, slot);
					ADS_DEBUG0((LP_ERROR, "external network %s banned by reseller blacklist\n",
								ADS_ENTITY_ID_CSTR(external_network_id)));
					continue;
				}

				if (!ctx->restriction_.reseller_whitelist_.empty()
					&& ctx->restriction_.reseller_whitelist_.find(external_network_id) == ctx->restriction_.reseller_whitelist_.end())
				{
					ad->log_selection_error(selection::Error_Code::RESELLER_WHITELIST_NOT_ALLOWED, slot);
					ADS_DEBUG0((LP_ERROR, "external network %s not allowed by reseller whitelist\n",
								ADS_ENTITY_ID_CSTR(external_network_id)));
					continue;
				}
			}

			if (!ad->is_net())
			{
				this->calculate_revenue_share_net_ecpm(ctx, slot, ad->network_id(), external_network_id, current_eCPM, current_eCPM);
			}
			const Ads_MRM_Rule *rule = nullptr;
			if (!this->pick_mrm_rule_for_external_ad(ctx, slot, *ad, rule, external_rule_ext_id, -1, check_price ? current_eCPM : -1, original_eCPM,
							ctx->is_watched(*ad) ? ctx->get_watched_id(*ad) : ads::entity::invalid_id(0)))
				continue;
			ADS_DEBUG0((LP_DEBUG, "Select external rule %s for ad %s\n", ADS_ENTITY_ID_CSTR(rule->id_), CANDIDATE_LITE_ID_CSTR(ad)));
			external_rule = rule;
		}

		if (mkpl_path != nullptr)
		{
			if (!mkpl_path->construct_eCPM_chain(ctx, current_eCPM, net_eCPMs))
			{
				ADS_DEBUG((LP_DEBUG, "ad %s is not met mkpl order floor price\n", ADS_ENTITY_ID_CSTR(ad->id())));
				if (ctx->is_watched(*ad))
				{
					ctx->log_watched_info(*ad, "MKPL_ORDER_FLOOR_PRICE_NOT_MET");
				}
				ctx->mkpl_executor_->log_ad_error(ctx, *ad, selection::Error_Code::MKPL_ORDER_FLOOR_PRICE_NOT_MET);
				return -1;
			}

			current_eCPM = net_eCPMs.front().bidding_eCPM_;
			max_path.mkpl_path_ = mkpl_path;
		}

		{
			std::list<Ads_Advertisement_Candidate::Net_ECPM_Info> subsequent_net_eCPMs;
			subsequent_net_eCPMs.swap(net_eCPMs);

			if (this->find_best_rule_path_for_advertisement(ctx, slot, candidate, current_eCPM, original_eCPM, check_price,
							max_eCPM, max_path.mrm_rule_path_, net_eCPMs, external_rule, external_rule_ext_id,
							mkpl_path != nullptr ? &mkpl_path->root_seller_closure() : nullptr) < 0)
			{
				continue;
			}

			if (!subsequent_net_eCPMs.empty())
			{
				ADS_ASSERT(max_path.mkpl_path_ != nullptr && subsequent_net_eCPMs.size() == max_path.mkpl_path_->size());
				ADS_ASSERT(net_eCPMs.size() == (max_path.mrm_rule_path_ != nullptr ? max_path.mrm_rule_path_->edges_.size() : 0) + 1);
				subsequent_net_eCPMs.pop_front();
				net_eCPMs.splice(net_eCPMs.end(), subsequent_net_eCPMs);
			}
		}

		//for mkpl audience extension traffic, the clearing eCPM of ad network should be the raw_eCPM
		if (!net_eCPMs.empty() && ctx->is_bidder_traffic())
		{
			net_eCPMs.back().clearing_eCPM_ = raw_eCPM;
		}

		if (max_eCPM < 0)
			continue;

		if (candidate->is_sponsorship(slot))
			candidate->replacable(slot, false);

		if (candidate->ad_unit() && candidate->ad_unit()->take_all_chances_) // FDB-3024
		{
			candidate->replacable(slot, false);
			size_t n_companions = candidate->companion_candidates_.size();
			for (size_t i = 0; i < n_companions; ++i)
				candidate->companion_candidates_[i]->replacable(slot, false);
		}

		// do NOT swap leader / follower for simplicity
		if (ad->is_leader() || ad->is_follower())
		{
			candidate->replacable(slot, false);
		}

		//TODO: cleanup
		candidate->net_eCPMs(slot) = net_eCPMs;
		candidate->access_path(slot) = max_path;
		candidate->external_rule(slot, external_rule, external_rule_ext_id);
		candidate->assoc_ = (ads::MEDIA_TYPE)(candidate->assoc_ | slot->associate_type());

		this->update_advertisement_candidate_priority(ctx, candidate, slot);

		candidate->dump();
		candidate->degrade_guaranteed_ad(ctx);

		bool slot_guaranteed = false;
		int64_t effective_eCPM = -1;
		selection::Effective_ECPM_Factors eCPM_factors;

		if (ad->operation_owner().is_content_right_owner(slot) || ad->operation_owner().guaranteed(slot))
		{
			if (own_eCPM_factors.effective_boost_ < 0 ||
				(external_rule != nullptr && external_rule->id_ != own_eCPM_factors.rule_id_)) /* cache boost & eCPM of pure OO ads */
			{
				own_effective_eCPM = this->calculate_effective_eCPM(ctx, *candidate, slot, &ad->operation_owner(), &own_eCPM_factors);
			}
			effective_eCPM = own_effective_eCPM;
			eCPM_factors = own_eCPM_factors;
			slot_guaranteed = true;
		}
		else if (max_path.mkpl_path_ != nullptr && max_path.mkpl_path_->execution_nodes_.size() > 1)
		{
			effective_eCPM = this->calculate_effective_eCPM(ctx, *candidate, slot, &max_path.mkpl_path_->execution_nodes_.back()->to_ad_owner(), &eCPM_factors);
		}
		else
		{
			//full reseller case
			effective_eCPM = this->calculate_effective_eCPM(ctx, *candidate, slot, nullptr, &eCPM_factors);
			candidate->rule_id(slot, eCPM_factors.rule_id_, eCPM_factors.rule_ext_id_);
		}

		candidate->effective_eCPM(slot, effective_eCPM, eCPM_factors.effective_boost_, slot_guaranteed);
		if (eCPM_factors.rule_fill_rate_ >= 0)
			candidate->rule_fill_rate(slot, (int)(eCPM_factors.rule_fill_rate_ * 100 + 0.5));

		found = true;
	}

	if (ad->is_watched() && own_eCPM_factors.effective_boost_ > 0)
	{
		json::ObjectP p;
		p["ad_boost"] = json::Number(own_eCPM_factors.ad_boost_);
		p["external_rule_boost"] = json::Number(own_eCPM_factors.rule_boost_);
		p["price_curve_id"] = json::Number(own_eCPM_factors.price_curve_id_);

		p["own_boost"] = json::Number(own_eCPM_factors.effective_boost_);
		p["own_eCPM"] = json::Number(ADS_TO_REAL_CURRENCY(own_eCPM_factors.base_eCPM_) * 1000.0);
		p["effective_eCPM"] = json::Number(ADS_TO_REAL_CURRENCY(own_effective_eCPM) * 1000.0);

		ctx->log_watched_info(ctx->get_watched_id(*ad), "boost_summary", p);
	}

	if (found)
	{
		int64_t internal_effective_eCPM = this->calculate_effective_eCPM(ctx, *candidate, nullptr, &ad->operation_owner(), nullptr);
		candidate->internal_effective_eCPM(internal_effective_eCPM);
		ADS_DEBUG((LP_DEBUG, "ad: %s, network internal eCPM: %lld \n", CANDIDATE_LITE_ID_CSTR(ad), internal_effective_eCPM));
		return 0;
	}

	delete candidate;
	candidate = 0;

	ctx->decision_info_.reject_ads_.put(*ad, Reject_Ads::NO_SUITABLE_PATH);
	ADS_DEBUG((LP_TRACE, "no suitable path for network %s:ad %s\n", ADS_ENTITY_ID_CSTR(ad->network_id()), CANDIDATE_LITE_ID_CSTR(ad)));
	return -1;
}

std::unique_ptr<Ads_MRM_Rule_Path>
Ads_Selector::select_mrm_rule_for_path(Ads_Selection_Context *ctx, const Ads_MRM_Rule_Path& path, Ads_Slot_Base *slot, const Ads_Advertisement_Candidate_Lite *ad,
					std::list<Ads_Advertisement_Candidate::Net_ECPM_Info> &eCPMs, int64_t eCPM, bool check_price)
{
	const Ads_MRM_Rule *dummy_external_rule = nullptr;
	Ads_GUID dummy_external_rule_ext_id = -1;
	return this->select_mrm_rule_for_path(ctx, path, slot, ad, eCPMs, eCPM, check_price, dummy_external_rule, dummy_external_rule_ext_id);
}

std::unique_ptr<Ads_MRM_Rule_Path>
Ads_Selector::select_mrm_rule_for_path(Ads_Selection_Context *ctx, const Ads_MRM_Rule_Path &path, Ads_Slot_Base *slot, const Ads_Advertisement_Candidate_Lite *ad,
					std::list<Ads_Advertisement_Candidate::Net_ECPM_Info> &eCPMs, int64_t eCPM, bool check_price,
					const Ads_MRM_Rule*& external_rule, Ads_GUID &external_rule_ext_id)
{
	auto new_path = std::make_unique<Ads_MRM_Rule_Path>(path);
	bool should_reactivated_rule = false;

	Ads_String reject_reason_str;
	bool found = true;

	std::list<Ads_Advertisement_Candidate::Net_ECPM_Info> path_eCPMs{eCPM};
	int64_t share = eCPM;

	const Ads_Repository *repo = ctx->repository_;

	const auto *ad_closure = ad->owner_.to_closure();
	bool is_path_through = (ad_closure != nullptr && path.terminal_partner_id() == ad_closure->network_id_);

	const Ads_MRM_Rule *partner_downstream_rule = nullptr;
	Ads_GUID partner_downstream_network_id = -1;
	if (is_path_through && external_rule != nullptr)
	{
		ADS_ASSERT(ad->is_external());
		partner_downstream_rule = external_rule;
		partner_downstream_network_id = ad->external_network_id();
	}

	//edge check and ecpm calculation
	auto edge_it = new_path->edges_.rbegin();
	for (; edge_it != new_path->edges_.rend(); ++edge_it)
	{
		//closure check
		const auto &edge = *edge_it;
		Ads_GUID partner_id = edge.partner_id();

		/// reseller blacklist
		if (ctx->restriction_.reseller_blacklist_.find(ads::entity::restore_looped_id(partner_id)) != ctx->restriction_.reseller_blacklist_.end())
		{
			ad->log_selection_error(selection::Error_Code::RESELLER_BLACKLIST_BANNED, slot);
			ADS_DEBUG((LP_ERROR, "reseller network %s banned by reseller blacklist\n", ADS_ENTITY_ID_CSTR(partner_id)));

			found = false;
			break;
		}

		if (!ctx->restriction_.reseller_whitelist_.empty()
			&& ctx->restriction_.reseller_whitelist_.find(ads::entity::restore_looped_id(partner_id)) == ctx->restriction_.reseller_whitelist_.end())
		{
			ad->log_selection_error(selection::Error_Code::RESELLER_WHITELIST_NOT_ALLOWED, slot);
			ADS_DEBUG((LP_ERROR, "reseller network %s not allowed by reseller whitelist\n", ADS_ENTITY_ID_CSTR(partner_id)));

			found = false;
			break;
		}

		const Ads_MRM_Rule* selected_rule = edge.rule_;
		ADS_ASSERT(selected_rule != nullptr);

		int64_t downstream_net_ecpm = share;
		bool margin_unsatisfied = false;

		const Ads_Marketplace::Margin* margin = Ads_Marketplace::get_marketplace_margin(*repo, partner_id, partner_downstream_network_id, *selected_rule, partner_downstream_rule, *ad->ad(), downstream_net_ecpm);
		if (this->calculate_net_ecpm_from_downstream(ctx, slot, *selected_rule, partner_id, downstream_net_ecpm, margin, margin_unsatisfied, share) < 0)
		{
			found = false;
			break;
		}
		if (!path_eCPMs.empty())
		{
			path_eCPMs.front().margin_ = margin;
		}

		if (check_price && margin_unsatisfied)
		{
			found = false;
			should_reactivated_rule = true;
			break;
		}

		path_eCPMs.emplace_front(share);

		auto upstream_nit = std::next(edge_it, 1);
		Ads_GUID upstream_network_id = -1;
		if (upstream_nit != new_path->edges_.rend())
			upstream_network_id = (*upstream_nit).network_id();

		Ads_String tmp_reject_reason;
		if (external_rule != nullptr && is_path_through && edge_it == new_path->edges_.rbegin())
		{
			selection::Error_Code error = selection::Error_Code::UNDEFINED;
			if (Ads_Asset_Section_Closure::is_rule_blocked_by_inbound_rule(ctx, external_rule, selected_rule, tmp_reject_reason, true, &error))
			{
				found = false;
				should_reactivated_rule = true;
				if (!tmp_reject_reason.empty()) reject_reason_str = tmp_reject_reason;
				ADS_DEBUG((LP_DEBUG, "external rule %s didn't match for current upstream rule %s\n", ADS_ENTITY_ID_CSTR(external_rule->id_), ADS_ENTITY_ID_CSTR(selected_rule->rule_id_)));
				if (error > selection::Error_Code::UNDEFINED)
					ad->log_selection_error(error, slot);
				break;
			}
		}
		if (!Ads_Asset_Section_Closure::is_rule_applicable_for_ad(ctx, selected_rule, slot, ad, upstream_network_id, partner_id, share, check_price, tmp_reject_reason))
		{
			found = false;
			should_reactivated_rule = true;
			if (!tmp_reject_reason.empty()) reject_reason_str = tmp_reject_reason;
		}

		partner_downstream_rule = selected_rule;
		partner_downstream_network_id = partner_id;
	}

	if (!found && should_reactivated_rule && !path.hard_guaranteed_through())
	{
		if (ad->is_external() && is_path_through)
		{
			std::vector<std::pair<const Ads_MRM_Rule*, Ads_GUID>> available_external_rules;
			const Ads_Advertisement *external_ad = ad->cast_to_ad();
			if (this->collect_available_external_rules(ctx, slot, *ad, available_external_rules))
			{
				for (const auto &available_external_rule : available_external_rules)
				{
					const Ads_MRM_Rule *repicked_external_rule = available_external_rule.first;
					path_eCPMs.clear();
					found = this->build_rule_path(ctx, slot, new_path.get(), eCPM, &path_eCPMs, ad, check_price, repicked_external_rule);
					if (found)
					{
						external_rule = repicked_external_rule;
						external_rule_ext_id = available_external_rule.second;
						ADS_DEBUG((LP_DEBUG, "select external rule %s for ad %s\n", ADS_ENTITY_ID_CSTR(external_rule->id_), CANDIDATE_LITE_ID_CSTR(ad)));
						break;
					}
				}
			}
		}
		else
		{
			path_eCPMs.clear();
			found = this->build_rule_path(ctx, slot, new_path.get(), eCPM, &path_eCPMs, ad, check_price);
		}
	}

	if (!found)
	{
		if (!reject_reason_str.empty() && ad->is_watched()) ctx->log_watched_info(*ad, slot, reject_reason_str);
	}

	new_path->has_reactivated_rule_ = should_reactivated_rule;
	eCPMs.swap(path_eCPMs);

	if (found) return new_path;
	return nullptr;
}

bool
Ads_Selector::collect_available_external_rules(Ads_Selection_Context *ctx, Ads_Slot_Base *slot, const Ads_Advertisement_Candidate_Lite &ad_lite,
						std::vector<std::pair<const Ads_MRM_Rule*, Ads_GUID>> &available_external_rules)
{
	ADS_ASSERT(ad_lite.is_external());

	Ads_GUID external_network_id = ad_lite.external_network_id();
	const Ads_Repository *repo = ctx->repository_;

	// for old mrm rule
	do
	{
		if (!ad_lite.target_mrm_rule_id().empty())
			break;
		const auto *closure = ad_lite.owner_.to_closure();
		if (closure == nullptr)
			break;
		auto it = closure->relations_.find(external_network_id);
		if (it == closure->relations_.end())
			break;
		const auto *relation = it->second;

		Ads_Asset_Section_Closure::Edge_MultiMap edges;
		edges.insert(relation->edges(ads::MEDIA_VIDEO).begin(),
			relation->edges(ads::MEDIA_VIDEO).end());
		edges.insert(relation->edges(ads::MEDIA_SITE_SECTION).begin(),
			relation->edges(ads::MEDIA_SITE_SECTION).end());

		for (auto const& eit : edges)
		{
			const auto *edge = eit.second;

			auto rit = edge->slot_rules_.find(slot);
			if (rit == edge->slot_rules_.end())
				continue;

			auto const& slot_rule = rit->second;
			if (slot_rule.reachable_)
			{
				Ads_GUID rule_id = slot_rule.rule_id_;
				const Ads_MRM_Rule *rule = nullptr;
				REPO_FIND_ENTITY_CONTINUE(repo, access_rule, rule_id, rule);
				if (rule->is_old_application_rule())
				{
					available_external_rules.push_back(std::make_pair(rule, slot_rule.rule_ext_id_));
				}
			}
    		}
	} while (0);

	const auto *slot_rules = ad_lite.owner_.available_partner_rules(external_network_id);
	if (slot_rules != nullptr)
	{
		const auto &rules = slot_rules->rules_of_slot(slot);
		for (const auto* rule : rules)
		{
			Ads_GUID rule_id = rule->rule_id_;
			if (!ad_lite.target_mrm_rule_id().empty() &&
				ad_lite.target_mrm_rule_id().find(rule_id) == ad_lite.target_mrm_rule_id().end())
			{
				continue;
			}

			available_external_rules.emplace_back(rule, 0);
		}
	}

	return !available_external_rules.empty();
}

bool
Ads_Selector::pick_mrm_rule_for_external_ad(Ads_Selection_Context *ctx, Ads_Slot_Base *slot, const Ads_Advertisement_Candidate_Lite &ad_lite,
					const Ads_MRM_Rule *&rule, Ads_GUID &rule_ext_id, Ads_GUID upstream_network_id,
					int64_t current_eCPM, int64_t raw_eCPM, Ads_GUID watched_id)
{
	const auto *ad = ad_lite.cast_to_ad();
	rule = nullptr;
	std::vector<std::pair<const Ads_MRM_Rule*, Ads_GUID>> available_external_rules;

	if (this->collect_available_external_rules(ctx, slot, ad_lite, available_external_rules))
	{
		Ads_String reject_reason_str;
		for (const auto &available_external_rule : available_external_rules)
		{
			const auto *external_rule = available_external_rule.first;
			ADS_ASSERT(external_rule != nullptr);

			Ads_String tmp_reject_reason;
			if (!Ads_Asset_Section_Closure::is_rule_applicable_for_ad(ctx, external_rule, slot, &ad_lite, upstream_network_id, ad_lite.external_network_id(),
					       					(current_eCPM >= 0 ? current_eCPM : 0), (current_eCPM >= 0), tmp_reject_reason, false))
			{
				if (reject_reason_str.empty() && !tmp_reject_reason.empty())
					reject_reason_str = tmp_reject_reason;
				continue;
			}

			rule = external_rule;
			rule_ext_id = available_external_rule.second;
			break;
		}

		if (rule != nullptr)
		{
			if (slot != nullptr)
				ADS_DEBUG0((LP_TRACE, "external ad %s use rule %s(on slot:%s)\n", ADS_ADVERTISEMENT_ID_CSTR(ad_lite.id()),
							ADS_RULE_CSTR(rule), slot->ad_unit_name_.c_str()));
		}
		else if (!reject_reason_str.empty() && ctx->is_watched(ad_lite))
			ctx->log_watched_info(ad_lite, slot, reject_reason_str);
	}

	if (rule == nullptr)
		return false;

	if (current_eCPM >= 0)
	{
		const Ads_MRM_Rule::Min_Price *min_price = ctx->min_price(rule, slot);
		if (min_price && min_price->type_ != Ads_MRM_Rule::MIN_PRICING_TYPE_NONE
			&& current_eCPM < min_price->value_)
		{
			ad_lite.log_selection_error(selection::Error_Code::PRICE_HURDLE_CHECK_FAILED, slot);
			if (ctx->is_watched(watched_id)) ctx->log_watched_info(watched_id, slot, "MIN_ECPM");
			ADS_DEBUG((LP_INFO, "eCPM for external ad %s (%s) share %s less than min eCPM %s of rule %s\n"
				, ADS_ENTITY_ID_CSTR(ad_lite.id()), ads::i64_to_str(raw_eCPM).c_str(), ads::i64_to_str(current_eCPM).c_str()
				, ads::i64_to_str(min_price->value_).c_str(), ADS_RULE_CSTR(rule)));
			return false;
		}
	}

	return true;
}

int
Ads_Selector::find_best_rule_path_for_advertisement(Ads_Selection_Context *ctx, Ads_Slot_Base *slot, const Ads_Advertisement_Candidate *candidate,
						int64_t current_eCPM, int64_t original_eCPM, bool check_price,
						int64_t& max_eCPM, const Ads_MRM_Rule_Path*& max_path,
						std::list<Ads_Advertisement_Candidate::Net_ECPM_Info>& net_eCPMs,
						const Ads_MRM_Rule*& external_rule, Ads_GUID& external_rule_ext_id,
						const Ads_Asset_Section_Closure *terminal_closure)
{
	// Use ad_lite as wrapper of ad
	Ads_Advertisement_Candidate_Lite *ad = candidate->lite_;
	Ads_Asset_Section_Closure *ad_closure = ad->owner_.to_closure();
	ADS_ASSERT(ctx && ctx->repository_);

	if (terminal_closure == nullptr)
	{
		ADS_ASSERT(ad_closure != nullptr);
		if (ad_closure == nullptr)
			return -1;
		terminal_closure = ad_closure;
	}

	bool is_path_through = (terminal_closure == ad_closure);
	ads::MEDIA_TYPE assoc = slot->associate_type();

	/// CRO
	if (terminal_closure->is_content_right_owner(slot))
	{
		if (terminal_closure == ad_closure && this->is_data_visibility_banned(ctx, nullptr, candidate, slot))
		{
			ad->log_selection_error(selection::Error_Code::DATA_VISIBILITY_BANNED, slot);
			ADS_DEBUG((LP_ERROR, "CRO's ad is banned by data right visibility\n"));
			return -1;
		}

		if (current_eCPM > max_eCPM)
		{
			max_eCPM = current_eCPM;
			net_eCPMs.clear();
			net_eCPMs.emplace_front(max_eCPM);
		}
		else
		{
			ad->log_selection_error(selection::Error_Code::INVALID_ECPM, slot);
			ADS_ERROR_RETURN((LP_ERROR, "invalid eCPM for ad %s? eCPM %s, current eCPM %s, max_eCPM %s\n",
			                  CANDIDATE_LITE_ID_CSTR(ad), ads::i64_to_str(original_eCPM).c_str(),
			                  ads::i64_to_str(current_eCPM).c_str(), ads::i64_to_str(max_eCPM).c_str()), -1);
		}

		return 0;
	}

	auto it = ctx->access_paths().find(slot);
	if (it == ctx->access_paths().end())
		return -1;

	Ads_MRM_Rule_Path_Map &paths = it->second;
	std::pair<const Ads_MRM_Rule_Path*, std::unique_ptr<Ads_MRM_Rule_Path>> max_path_pair;

	Ads_MRM_Rule_Path_Map::iterator first = paths.lower_bound(terminal_closure->network_id_);
	Ads_MRM_Rule_Path_Map::iterator last = paths.upper_bound(terminal_closure->network_id_);
	for (; first != last; ++first)
	{
		const Ads_MRM_Rule *temp_external_rule = nullptr;
		Ads_GUID temp_external_rule_ext_id = ads::entity::make_id(ADS_ENTITY_TYPE_MRM_RULE, 0);
		std::list<Ads_Advertisement_Candidate::Net_ECPM_Info> eCPMs;

		int64_t eCPM = current_eCPM;

		const Ads_MRM_Rule_Path *path = first->second;
		if (path->has_reactivated_rule_) continue;

		//restrict data visibility to resellers
		bool should_reactivate_rule = true;
		if (this->is_data_visibility_banned(ctx, path, candidate, slot, should_reactivate_rule) && !should_reactivate_rule)
		{
			ad->log_selection_error(selection::Error_Code::DATA_VISIBILITY_BANNED, slot);
			continue;
		}

		if (is_path_through)
		{
			Ads_GUID upstream_network_id;
			if (path->edges_.size() > 1)
			{
				auto it = path->edges_.rbegin();
				upstream_network_id = (*(++it)).partner_id();
			}
			else
				upstream_network_id = ctx->root_network_id(assoc);

			if (ad->is_external())
			{
				const Ads_MRM_Rule *rule = nullptr;
				if (!this->pick_mrm_rule_for_external_ad(ctx, slot, *ad, rule, temp_external_rule_ext_id, upstream_network_id, check_price ? eCPM : -1, original_eCPM))
				{
					continue;
				}

				temp_external_rule = rule;
			}
			else if (ads::entity::is_valid_id(upstream_network_id) &&
				!Ads_Asset_Section_Closure::is_allowed_upstream_network_for_ad(ad->cast_to_ad(), upstream_network_id, ctx->repository_, ctx->delta_repository_))
			{
				ad->log_selection_error(selection::Error_Code::NOT_ALLOWED_UPSTREAM_NETWORK_FOR_AD, slot);
				continue;
			}
		}
		else
		{
			temp_external_rule = external_rule;
			temp_external_rule_ext_id = external_rule_ext_id;
		}

		auto selected_path = this->select_mrm_rule_for_path(ctx, *path, slot, ad, eCPMs, eCPM, check_price, temp_external_rule, temp_external_rule_ext_id);
		if (!selected_path || selected_path->edges_.empty()) continue;

		ADS_ASSERT(eCPMs.size() == selected_path->edges_.size() + 1);

		///XXX: inefficient
		double rule_fill_rate = -1.0;
		double compensation = this->calculate_mrm_rule_boost_factor(ctx, selected_path->edges_.front().rule_id_, selected_path->edges_.front().rule_ext_id_, rule_fill_rate);
		int64_t compensated_eCPM = int64_t(eCPMs.front().bidding_eCPM_ * compensation);

		if (Ads_Server_Config::instance()->enable_debug_)
		{
			ADS_DEBUG((LP_DEBUG, "Ad(%s) net eCPM: %lld, boost factor: %f, effective eCPM: %lld, when using path: ",
						CANDIDATE_LITE_LPID_CSTR(ad),
						eCPMs.front().bidding_eCPM_,
						compensation,
						compensated_eCPM));
			selected_path->dump();
		}

		if (compensated_eCPM > max_eCPM)
		{
			max_eCPM = compensated_eCPM;
			//selected_path would be set to nullptr after move.
			max_path_pair = std::make_pair(path, std::move(selected_path));
			net_eCPMs.swap(eCPMs);

			if (temp_external_rule != external_rule)
			{
				external_rule = temp_external_rule;
				external_rule_ext_id = temp_external_rule_ext_id;
			}
		}
	}

	if (max_path_pair.second && max_path_pair.second->has_reactivated_rule_)
	{
		Ads_MRM_Rule_Path *new_path = max_path_pair.second.release();
		paths.emplace(terminal_closure->network_id_, new_path);
		max_path = new_path;
	}
	else
		max_path = max_path_pair.first;

	if (max_eCPM < 0)
		return -1;

	return 0;
}

#if defined(ADS_ENABLE_FORECAST)
int
Ads_Selector::find_best_path_for_advertisement(Ads_Selection_Context *ctx, Ads_Asset_Section_Closure *closure, Ads_Slot_Base *slot, const Ads_Advertisement_Candidate *candidate, const Ads_MRM_Rule_Path *&best_path)
{
	ADS_ASSERT(ctx && ctx->repository_);
	ADS_ASSERT(closure && closure->slot_references_.find(slot) != closure->slot_references_.end());

	Ads_Asset_Section_Closure::Slot_Ref &slot_ref = closure->slot_ref(slot);
	if (slot_ref.sources_.empty()) // CRO
	{
		if (ctx->data_right_management_->is_data_right_enabled() && this->is_data_visibility_banned(ctx, NULL, candidate, slot))
		{
			ADS_DEBUG((LP_ERROR, "CRO's ad is banned by data right visibility"));
			return -1;
		}
		return 0;
	}
	const Ads_Advertisement_Candidate_Lite *ad = candidate->lite_;
	bool is_custom_portfolio = ads::entity::type(ad->id()) == ADS_ENTITY_TYPE_ADVERTISEMENT;

	if (!is_custom_portfolio && ctx->data_right_management_->data_right_of_full_network(closure->network_id_).is_all_fields_target_enabled()) return 0;

	ads::MEDIA_TYPE assoc = slot->associate_type();
	Ads_MRM_Rule_Path_Map &paths = ctx->access_paths()[slot];
	Ads_MRM_Rule_Path_Map::iterator first = paths.lower_bound(closure->network_id_);
	Ads_MRM_Rule_Path_Map::iterator last = paths.upper_bound(closure->network_id_);
	for (; first != last; ++first)
	{
		const Ads_MRM_Rule_Path *path = first->second;
		if (path->has_reactivated_rule_)
			continue;
		Ads_GUID upstream_network_id = ads::entity::make_id(ADS_ENTITY_TYPE_NETWORK, 0);
		if (is_custom_portfolio)
		{
			if (path->edges_.size() > 1)
			{
				auto it = path->edges_.rbegin();
				upstream_network_id = (*(++it)).partner_id();
			}
			else
				upstream_network_id = ctx->root_network_id(assoc);
		}
		bool should_reactivate_rule = true;
		if (this->is_data_visibility_banned(ctx, path, candidate, slot, should_reactivate_rule) && !should_reactivate_rule)
			continue;
		if (is_custom_portfolio && !Ads_Asset_Section_Closure::is_allowed_upstream_network_for_ad(ad->cast_to_ad(), upstream_network_id, ctx->repository_, ctx->delta_repository_))
			continue;
		std::list<Ads_Advertisement_Candidate::Net_ECPM_Info> eCPMs;
		auto selected_path = select_mrm_rule_for_path(ctx, *path, slot, ad, eCPMs, 0, false);
		if (selected_path != nullptr)
		{
			if (selected_path->has_reactivated_rule_)
			{
				Ads_MRM_Rule_Path *new_path = selected_path.release();
				//add best_path to ctx to avoid memory leak
				paths.insert(std::make_pair(closure->network_id_, new_path));
				best_path = new_path;
			}
			else
				best_path = path;
			break;
		}
	}
	if (first == last)
	{
		return -1;
	}
	return 0;
}
#endif

#if defined(ADS_ENABLE_FORECAST)
void add_proposal_for_hylda_incompatible_candidate(size_t& flag, Ads_Advertisement_Candidate* candidate)
{
	if (flag & Ads_Request::Smart_Rep::PROPOSAL_IF_NO_BUDGET
			&& candidate->slot_ && candidate->slot_->env() == ads::ENV_VIDEO)
	{
		Ads_Video_Slot* vslot = reinterpret_cast<Ads_Video_Slot*>(candidate->slot_);
		if (vslot->flags_ & Ads_Slot_Base::FLAG_STATIC_SCHEDULE
				&& candidate->ad_->targeting_type_ == Ads_Advertisement::HYLDA_INCOMPATIBLE)
			candidate->flags_ |= Ads_Advertisement_Candidate::FLAG_PROPOSAL;
	}
}
#endif

int
Ads_Selector::take_companion(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate *candidate, Ads_Advertisement_Candidate_Vector& companion_candidates,
			Ads_Advertisement_Candidate_Set& filters, std::vector<Ads_Slot_Base *>& plan, size_t& flags, int64_t& profit, bool is_taking_scheduled_ad_companion)
{
	size_t n_companions = companion_candidates.size();

	/// count number of video ads, and add it to display slot
	size_t n_video_advertisements = 0;
	for (size_t i = 0; i < n_companions; ++i)
	{
		Ads_Slot_Base *slot = plan[i];
		if (slot && slot->env() == ads::ENV_VIDEO)
			n_video_advertisements++;
	}

	for (size_t i = 0; i < n_companions; ++i)
	{
		Ads_Slot_Base *slot = plan[i];
		Ads_Advertisement_Candidate *current = companion_candidates[i];

		if (!slot) continue;

		if (!current->slot_) current->slot_ = slot;

		switch (slot->env())
		{
			case ads::ENV_VIDEO:
			{
				if (is_taking_scheduled_ad_companion)
				{
					continue;
				}
				reinterpret_cast<Ads_Video_Slot*>(slot)->take_advertisement(ctx, current);
				if (current->is_pod_ad())
				{
					try_fill_ad_pod(ctx, current, slot, filters, profit);
				}
			}

			break;
			case ads::ENV_PLAYER:
			case ads::ENV_PAGE:
			{
				if (n_video_advertisements == 0)
					ctx->use_slot(current, slot, Ads_Selection_Context::STANDALONE);
				else if (flags & FLAG_CHECK_INITIAL)
				{
					ctx->use_slot(current, slot, Ads_Selection_Context::INITIAL_COMPANION);
					slot->load_order_ = "i";
				}
				else
					ctx->use_slot(current, slot, Ads_Selection_Context::NORMAL_COMPANION);
			}

			break;

			default:
				break;
		}

		ctx->fc_->inc_occurences(current, slot, ctx);
		filters.insert(current);

		if (!current->ad_->budget_exempt_)
			profit += current->effective_eCPM();
	}
	return 0;
}
void
Ads_Selector::fill_companion(Ads_Selection_Context* ctx, Ads_GUID companion_id, Ads_Slot_List& slots, Ads_Advertisement_Candidate_Set &filters,
				Ads_Advertisement_Candidate_Vector &companion_candidates, std::vector<Ads_Slot_Base*> &plan,
				std::map<const Ads_Advertisement_Candidate*, Ads_Slot_Base*> &filled_candidates, std::set<Ads_Advertisement_Candidate *> &processed_candidates,
				std::multimap<Ads_Slot_Base *, Ads_Advertisement_Candidate *> &occupied, size_t &flags, bool &recoverable, bool &has_video_ad)
{
	size_t n_companions = companion_candidates.size();
	std::map<Ads_Slot_Base *, const Ads_Advertisement_Candidate *> companion_leaders;
	for (size_t i = 0; i < n_companions; ++i)
	{
		plan[i] = 0;
		Ads_Advertisement_Candidate *current = companion_candidates[i];
		processed_candidates.insert(current);

		if (!current->valid_) continue;

		Ads_Slot_Base *slot = 0;
		bool find_applicable_slot = false;
		if (filled_candidates.find(current) == filled_candidates.end())
		{
			if (current->ad_unit()->is_temporal())
			{
				has_video_ad = true;
				std::vector<Ads_Slot_Base*> vect_recoverable;
				if (this->find_slot_for_advertisement(ctx, current, slots, filters, occupied, slot, flags,
							&companion_leaders, &vect_recoverable) >= 0 && slot)
				{
					//temp_recoverable_info.active_slot(slot);
					if (current->is_multiple_ads_) {
						slot->filled_by_multi_ads_ = true;
						current->replacable(slot, false);
					}

					find_applicable_slot = true;

					if (ctx->request_->rep()->site_section_.video_player_.video_.auto_play_
							&& !ctx->keep_initial_ad()
							//&& reinterpret_cast<Ads_Video_Slot *>(slot)->time_position_ <= MAX_INITIAL_TIME_POSITION)
						&& !slot->force_first_call()
							&& int64_t(reinterpret_cast<Ads_Video_Slot *>(slot)->time_position_) <= ctx->max_initial_time_position_)
							flags |= FLAG_CHECK_INITIAL;

					if (current->ad_->is_leader())
					{
						Ads_Video_Slot *aslot = reinterpret_cast<Ads_Video_Slot *>(slot);
						ADS_ASSERT(aslot);

						companion_leaders.insert(std::make_pair(aslot, current));
						if (aslot->parent_) companion_leaders.insert(std::make_pair(aslot->parent_, current));
					}
				}
				else
				{
					if (!vect_recoverable.empty())
					{
						recoverable = true;
					}
				}
			}
			else
			{
				flags |= FLAG_CHECK_COMPANION;
				if ((this->find_slot_for_advertisement(ctx, current, ctx->player_slots_, filters, occupied, slot, flags) >= 0 && slot)
						|| (this->find_slot_for_advertisement(ctx, current, ctx->page_slots_, filters, occupied, slot, flags) >= 0 && slot)
				   )
					find_applicable_slot = true;
			}
		}
		if (find_applicable_slot)
		{
			auto it = occupied.insert(std::make_pair(slot, current));
			if (!ctx->fc_->pass_frequency_cap_by_slot(ctx, current, true, &occupied))
			{
				occupied.erase(it);
				ctx->decision_info_.add_values(Decision_Info::INDEX_0);
				ADS_DEBUG((LP_DEBUG, "fill_companion, ad: %s, failed frequency cap, drop it, slot %s\n", CANDIDATE_LITE_LPID_CSTR(current->lite_), slot->custom_id_.c_str()));
				continue;
			}
			if (current && current->ad_ && slot)
				ADS_DEBUG((LP_DEBUG, "fill_companion, ad: %s, find valid slot, take it, slot %s\n", CANDIDATE_LITE_LPID_CSTR(current->lite_), slot->custom_id_.c_str()));
			plan[i] = slot;
			continue;
		}
		else if (filled_candidates.find(current) != filled_candidates.end())
		{
			plan[i] = filled_candidates[current];
			if (current->ad_->is_leader())
			{
				Ads_Video_Slot *aslot = reinterpret_cast<Ads_Video_Slot *>(plan[i]);
				companion_leaders.insert(std::make_pair(aslot, current));
				if (aslot->parent_) companion_leaders.insert(std::make_pair(aslot->parent_, current));
			}
			continue;
		}

		if (!find_applicable_slot && current->lite_ != nullptr)
		{
			ctx->decision_info_.reject_ads_.put(*(current->lite_), Reject_Ads::SLOT_NOT_FOUND, current->lite_->sub_reject_reason_bitmap());
		}

		ADS_DEBUG((LP_DEBUG, "%s ad: %s in companion package: %s not able to deliver\n", current->ad_->ad_unit()->time_position_class_.c_str(), ADS_ENTITY_ID_CSTR(current->ad_->id_), ADS_ENTITY_ID_CSTR(companion_id)));
	}
}
void
Ads_Selector::complete_fill_companion(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate *candidate, bool satisfied, bool recoverable,
					Ads_Advertisement_Candidate_Set &filters, std::vector<Ads_Slot_Base*> &plan, size_t &flags, int64_t &profit,
					std::set<Ads_Advertisement_Candidate*> &processed_candidates, Retry_Scheduler &candidate_combine_with_recover,
					Ads_Advertisement_Candidate_List &repeat_candidates)
{
	size_t n_companions = candidate->companion_candidates_.size();
	if (satisfied)
	{
		satisfied = false;
		for (size_t i = 0; i < n_companions; i++)
		{
			Ads_Advertisement_Candidate *current = candidate->companion_candidates_[i];
			if (plan[i] && !current->ad_->budget_exempt_) {satisfied = true; break;}
		}
		if (!satisfied && !recoverable)//TODO: why set it invalid? zlin
		{
			for (size_t i = 0; i < n_companions; i++)
				candidate->companion_candidates_[i]->valid_ = false;
			ADS_DEBUG((LP_DEBUG, "companion package: %s not able to deliver,bc no candidate ad count against budget\n", ADS_ENTITY_ID_CSTR(candidate->ad_->placement_id())));
		}
	}
	if (!satisfied)
	{
		if (recoverable)
		{
			candidate_combine_with_recover.pend_for_retry(candidate);
			processed_candidates.erase(candidate);
		}
		else if (ctx->decision_info_.reject_ads_.is_sample())
		{
			for (size_t i = 0; i < n_companions; i++)
			{
				auto *current = candidate->companion_candidates_[i];
				if (plan[i] != nullptr && !current->ad_->budget_exempt_ && current->lite_ != nullptr)
					ctx->decision_info_.reject_ads_.put(*(current->lite_), Reject_Ads::FILL_COMPANION);
			}
		}
		flags = 0;
	}
	else
	{
		this->take_companion(ctx, candidate, candidate->companion_candidates_, filters, plan, flags, profit);
		candidate_combine_with_recover.reactivate();

		/// record replication
		for (size_t i = 0; i < n_companions; ++i)
		{
			Ads_Slot_Base *slot = plan[i];
			Ads_Advertisement_Candidate *current = candidate->companion_candidates_[i];

			if (!slot) continue;

			if (!current->valid_) continue;

			if (current->ad_->repeat_mode_ == Ads_Advertisement::REPEAT_EACH_BREAK
					|| current->ad_->ad_unit()->take_all_chances_
					|| (current->ad_->is_sponsorship() && current->ad_->repeat_mode_ == Ads_Advertisement::REPEAT_DEFAULT)
#if defined(ADS_ENABLE_FORECAST)
					|| ctx->request_flags() & Ads_Request::Smart_Rep::BYPASS_BUDGET_CHECK
#endif
					|| current->ad_->is_follower())
			{
				repeat_candidates.push_back(current);
				current->used_slots_.insert(slot);
			}
		}
	}
}

bool
Ads_Selector::enable_improve(const Ads_Selection_Context *ctx, int step) const
{
	const auto &network_improve_steps_map = ctx->repository_->system_config().network_improve_steps_;
	if (network_improve_steps_map.empty()) // no such configure
	{
		return true;
	}

	auto it = network_improve_steps_map.find(ads::entity::id(ctx->root_network_id()));
	if (it != network_improve_steps_map.end())
	{
		return (it->second & (1 << step)) != 0;
	}

	it = network_improve_steps_map.find(-1);
	if (it != network_improve_steps_map.end())
	{
		return (it->second & (1 << step)) != 0;
	}

	return false;
}

Ads_Advertisement_Candidate*
selection::Advertisement_Owner_Base::Unified_Yield::find_unified_yield_candidate(Ads_Selection_Context &ctx, Ads_Advertisement_Candidate *soft_guaranteed_ad, const Ads_Slot_Base *slot, std::function<bool(Ads_Advertisement_Candidate*)> checker)
{
	const auto *soft_guaranteed_ad_owner = &soft_guaranteed_ad->owner_;
	const auto soft_guaranteed_ad_ecpm = soft_guaranteed_ad->net_eCPM(slot).bidding_eCPM_;
	const auto soft_guaranteed_ad_duration = soft_guaranteed_ad->duration(slot);
	for (auto it = unified_yield_candidates_.begin(); it != unified_yield_candidates_.end(); ++it)
	{
		auto *uf_ad = *it;
		if (soft_guaranteed_ad_ecpm >= uf_ad->net_eCPM(slot).bidding_eCPM_ || soft_guaranteed_ad_duration < uf_ad->duration(slot))
			continue;
		if (!checker(uf_ad)) continue;
		unified_yield_candidates_.erase(it);
		ctx.unified_yield_replaced_soft_guaranteed_candidates_.insert(soft_guaranteed_ad);
		ctx.unified_yield_filled_candidates_.insert(uf_ad);
		uf_ad->replaced_guaranteed_ad_ = soft_guaranteed_ad;
		uf_ad->replaced_slot_ = slot;
		uf_ad->replacable(slot, false);
		soft_guaranteed_ad->replacable(slot, false);
		return uf_ad;
	}
	return nullptr;
}

bool
selection::Advertisement_Owner_Base::Unified_Yield::is_applicable_for_unified_yield() const {
	return is_enabled_ && !unified_yield_candidates_.empty();
}

void
selection::Advertisement_Owner_Base::Unified_Yield::reset() {
	unified_yield_candidates_.clear();
	is_enabled_ = false;
}

//GREEDYS
int
Ads_Selector::knapsack_initialize_solution(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate_List& candidates, Ads_Slot_List& slots, Ads_Advertisement_Candidate_Set& filters, size_t aflags, int64_t& profit)
{
#if defined(ADS_ENABLE_FORECAST)
	Take_Advertisement_Slot_Position* slot_position = new Take_Advertisement_Slot_Position();
	std::vector<Ads_Advertisement_Candidate *> tmp;
	for (Ads_Advertisement_Candidate_List::const_iterator it = candidates.begin(); it != candidates.end(); ++it)
		tmp.push_back(*it);
	std::stable_sort(tmp.begin(), tmp.end(), Ads_Advertisement_Candidate::greater(ctx, ads::MEDIA_VIDEO, false /*ignore_duration*/, true /*ignore_active*/));
	Take_Advertisement_Candidate_Rank* candidate_rank = new Take_Advertisement_Candidate_Rank();
	for (size_t i = 0; i < tmp.size(); ++i)
		(*candidate_rank)[tmp[i]] = tmp.size() - i;
#endif

	//size_t n_candidates = candidates.size();
	std::set<Ads_Advertisement_Candidate *> processed_candidates;
	bool check_companion = true;
	if (ctx->request_->has_explicit_candidates()) check_companion = ctx->request_->rep()->ads_.check_companion_;

	//for (size_t i=0; i<n_candidates; ++i)
	Retry_Scheduler candidate_combine_with_recover(candidates, ctx);
	while(true)
	{
		Ads_Advertisement_Candidate *candidate = candidate_combine_with_recover.get_next_candidate();
		if (candidate == nullptr)
		{
			const auto &pend_candidates = candidate_combine_with_recover.get_pend_candidates();
			for(const auto *candidate : pend_candidates)
			{
				if(candidate->lite_->sub_reject_reason_bitmap() > 0)
					ctx->decision_info_.reject_ads_.put(*candidate->lite_, Reject_Ads::SLOT_NOT_FOUND,
				                                        candidate->lite_->sub_reject_reason_bitmap());
			}
			break;
		}

		if (ctx->unified_yield_filled_candidates_.find(candidate) != ctx->unified_yield_filled_candidates_.end())
		{
			processed_candidates.insert(candidate);
			continue;
		}

		if (candidate->is_follower_pod_ad())
			continue;

		size_t flags = aflags;
		if (!candidate->ad_unit()->is_temporal())
		{
			// FDB-4567
			if (!candidate->active(ads::MEDIA_VIDEO)) continue;
			if (!candidate->is_guaranteed(ads::MEDIA_VIDEO) && (!candidate->priority(ads::MEDIA_VIDEO) || candidate->priority(ads::MEDIA_VIDEO)[0] <= 0)) continue;
			if (!candidate->is_companion())
			{
				Ads_Slot_Base *slot = 0;
				std::multimap<Ads_Slot_Base *, Ads_Advertisement_Candidate *> occupied;
				if (!ctx->fc_->pass_frequency_cap_by_slot(ctx, candidate))
				{
					ctx->decision_info_.add_values(Decision_Info::INDEX_0);
					continue;
				}
				if ((this->find_slot_for_advertisement(ctx, candidate, ctx->player_slots_, filters, occupied, slot, flags|FLAG_CHECK_STANDALONE) >= 0 && slot)
				//		|| (this->find_slot_for_advertisement(ctx, candidate, ctx->page_slots_, filters, occupied, slot, flags) >= 0 && slot)
				   )
				{
					if (slot->no_standalone_if_temporal()) continue; //XXX: we dont know yet, so continue, need redesign
					if (!candidate->slot_) candidate->slot_ = slot;
					filters.insert(candidate);
					candidate->slot_->load_order_ = "i";

					ctx->use_slot(candidate, slot, Ads_Selection_Context::STANDALONE);

					if (!candidate->is_proposal())
					{
						/// mark filled
						flags = FLAG_CHECK_PROPOSAL_ONLY; //TODO only used by forecast, deprecated
					}

					/// frequency capping
					ctx->fc_->inc_occurences(candidate, slot, ctx);

					profit += candidate->effective_eCPM();
					continue;
				}
			}
			else
			{
				bool has_temporal = false;
				for (Ads_Advertisement_Candidate_Vector::const_iterator cit = candidate->companion_candidates_.begin();
					 cit != candidate->companion_candidates_.end(); ++cit)
				{
					const Ads_Advertisement_Candidate *current = *cit;
					if (current->ad_unit() && current->ad_unit()->is_temporal())
					{
						has_temporal = true;
						break;
					}
				}
				if (has_temporal) continue;
				else
				{
					//for companion of non-temporal ads, need to check if targeting slots allow intial ads
					flags |= FLAG_CHECK_INITIAL;
				}
			}
		}

		if (processed_candidates.find(candidate) != processed_candidates.end()) continue;
		processed_candidates.insert(candidate);

		Ads_Advertisement_Candidate_List repeat_candidates;

		if (check_companion && candidate->is_companion(true/*ignore companion size*/))
		{
			size_t n_companions = candidate->companion_candidates_.size();
			std::vector<Ads_Slot_Base *> plan(n_companions);
			std::multimap<Ads_Slot_Base *, Ads_Advertisement_Candidate *> occupied;
			std::map<const Ads_Advertisement_Candidate *, Ads_Slot_Base *> filled_candidates;
			Ads_GUID placement_id = candidate->ad_->placement_id();
			const Ads_Advertisement *placement = 0;
			if (!candidate->ad_->is_old_external())
			{
				if (ctx->find_advertisement(placement_id, placement) < 0 || !placement) continue;
			}

			if (placement && placement->advanced_companion_ && placement->companion_mode_ == Ads_Advertisement::NOT_LINKED)
			{
				bool satisfied = false, recoverable = false, has_video_ad = false, has_video_ad_filled = false;
				for (Linking_Set_List_Vector::const_iterator sit = ctx->placement_custom_sets_[placement_id].begin(); sit != ctx->placement_custom_sets_[placement_id].end(); ++sit)
				{
					bool companion_satisfied = true;
					Ads_GUID companion_id = sit->first;
					Ads_Advertisement_Candidate_Vector custom_set_companion_candidates;
					custom_set_companion_candidates.assign(sit->second.begin(), sit->second.end());
					size_t n_custom_set_companions = custom_set_companion_candidates.size();
					std::vector<Ads_Slot_Base *> custom_set_plan(n_custom_set_companions);
					fill_companion(ctx, companion_id, slots, filters, custom_set_companion_candidates, custom_set_plan, filled_candidates, processed_candidates, occupied, flags, recoverable, has_video_ad);
					bool is_custom_set = false;
					for (Ads_Advertisement::Ad_Linking_Group_RVector::const_iterator lit = placement->ad_linking_groups_.begin(); lit != placement->ad_linking_groups_.end(); ++lit)
					{
						if (lit->first == companion_id)
						{
							is_custom_set = true;
							break;
						}
					}
					if (is_custom_set)
						companion_satisfied = validate_advertisement_companion(ctx, custom_set_companion_candidates, custom_set_plan, companion_id);

					if (companion_satisfied)
					{
						for (size_t i = 0; i < n_custom_set_companions; i++)
						{
							Ads_Advertisement_Candidate *current = custom_set_companion_candidates[i];
							if (custom_set_plan[i] && filled_candidates.find(current) == filled_candidates.end())
							{
								filled_candidates.insert(std::make_pair(current, custom_set_plan[i]));
								if (current->ad_unit()->is_temporal())
									has_video_ad_filled = true;
							}
						}
					}
					else//companion failed, remove occupied
					{
						for (size_t i = 0; i < n_custom_set_companions; i++)
						{
							Ads_Advertisement_Candidate *current = custom_set_companion_candidates[i];
							if (custom_set_plan[i] && filled_candidates.find(current) == filled_candidates.end())
							{
								std::pair<std::multimap<Ads_Slot_Base*, Ads_Advertisement_Candidate*>::iterator, std::multimap<Ads_Slot_Base*, Ads_Advertisement_Candidate*>::iterator> iterpair = occupied.equal_range(custom_set_plan[i]);
								for (std::multimap<Ads_Slot_Base*, Ads_Advertisement_Candidate*>::iterator oit = iterpair.first; oit != iterpair.second; ++oit)
								{
									if (oit->second == current)
									{
										occupied.erase(oit);
										break;
									}
								}
							}
						}
					}
				}//per custom set
				if (has_video_ad && !has_video_ad_filled)
				{
					filled_candidates.clear();
					ADS_DEBUG((LP_DEBUG, "companion placement %s can not serve, bc no video ad can server\n", ADS_ENTITY_ID_CSTR(placement_id)));
				}
				if (!filled_candidates.empty())
				{
					//put filled candidates into plan
					for (size_t i = 0; i < n_companions; i++)
					{
						Ads_Advertisement_Candidate *current = candidate->companion_candidates_[i];
						if (filled_candidates.find(current) != filled_candidates.end())
							plan[i] = filled_candidates[current];
					}
					satisfied = true;
				}
				this->complete_fill_companion(ctx, candidate, satisfied, recoverable, filters, plan, flags, profit, processed_candidates, candidate_combine_with_recover, repeat_candidates);
			}
			else
			{
				bool satisfied = false, recoverable = false, has_video_ad = false;
				Ads_GUID companion_id = candidate->ad_->is_old_external() ? candidate->ad_->companion_id_
					: candidate->ad_->placement_id();
				fill_companion(ctx, companion_id, slots, filters, candidate->companion_candidates_, plan, filled_candidates, processed_candidates, occupied, flags, recoverable, has_video_ad);
				satisfied = this->validate_advertisement_companion(ctx, candidate->companion_candidates_, plan);
				this->complete_fill_companion(ctx, candidate, satisfied, recoverable, filters, plan, flags, profit, processed_candidates, candidate_combine_with_recover, repeat_candidates);
			}
		}
		else
		{
			Ads_Slot_Base *slot = 0;
			std::multimap<Ads_Slot_Base *, Ads_Advertisement_Candidate *> dummy;

			if (candidate->ad_->budget_exempt_) continue;//stand alone ad should NOT be set budget exempt.

			if (!ctx->fc_->pass_frequency_cap_by_slot(ctx, candidate))
			{
				ctx->decision_info_.add_values(Decision_Info::INDEX_0);
				ctx->decision_info_.reject_ads_.put(*candidate->lite_, Reject_Ads::FREQ_CAP);
				continue;
			}

			std::vector<Ads_Slot_Base*> vect_slot;
			if (this->find_slot_for_advertisement(ctx, candidate, slots, filters, dummy, slot, flags, 0, &vect_slot) < 0 || !slot)
			{
				if (vect_slot.size())
				{
					candidate_combine_with_recover.pend_for_retry(candidate);
					processed_candidates.erase(candidate);
				}
				else
				{
					ctx->decision_info_.add_values(Decision_Info::INDEX_2);
					// TODO: miss rejected hylda dynamic ad
					if (ctx->scheduler_->need_replace_scheduled_ad())
					{
						candidate->lite_->reset_sub_reject_reason_bitmap();
					}
					else
					{
						ctx->decision_info_.reject_ads_.put(*candidate->lite_, Reject_Ads::SLOT_NOT_FOUND,
						                                    candidate->lite_->sub_reject_reason_bitmap());
					}
				}
				continue;
			}

			do
			{
				if (!ctx->is_unified_yiled_enabled_)
					break;
				if (candidate->soft_guaranteed_version_ != Ads_Advertisement_Candidate::SOFT_GUARANTEED_VERSION::V2)
					break;
				auto &unified_yield = candidate->owner_.unified_yield_;
				if (!unified_yield.is_applicable_for_unified_yield())
					break;

				std::function<bool(Ads_Advertisement_Candidate*)> checker = [&](Ads_Advertisement_Candidate *can_ad)
				{
					Ads_Slot_Base *backup_slot = nullptr;
					if (!ctx->fc_->pass_frequency_cap_by_slot(ctx, can_ad))
						return false;
					int ret = this->is_slot_applicable_for_advertisement(ctx, can_ad, slots, filters, dummy, slot, backup_slot, flags, 0, nullptr);
					if (ret == -1)
						return false;
					return ret == 0 || backup_slot == slot;
				};

				auto *uf_ad = unified_yield.find_unified_yield_candidate(*ctx, candidate, slot, checker);
				if (!uf_ad)
					break;
				ADS_DEBUG((LP_DEBUG, "%s is replaced by %s at %s by unified yield 2.0\n", CANDIDATE_LITE_ID_CSTR(candidate->lite_), CANDIDATE_LITE_ID_CSTR(uf_ad->lite_), slot->custom_id_.c_str()));
				processed_candidates.erase(candidate);
				processed_candidates.insert(uf_ad);
				candidate = uf_ad;
			} while (false);

			candidate->slot_ = slot;
			if (candidate->is_multiple_ads_) {
				slot->filled_by_multi_ads_ = true;
				candidate->replacable(slot, false);
			}


#if defined(ADS_ENABLE_FORECAST)
			if (ctx->scheduler_->is_digital_live_schedule())
				add_proposal_for_hylda_incompatible_candidate(ctx->request_->rep()->flags_, candidate);
#endif

			reinterpret_cast<Ads_Video_Slot*>(slot)->take_advertisement(ctx, candidate);
			if (candidate->is_pod_ad())
			{
				try_fill_ad_pod(ctx, candidate, slot, filters, profit);
			}
			candidate_combine_with_recover.reactivate();
			filters.insert(candidate);

			if (candidate->ad_->repeat_mode_ == Ads_Advertisement::REPEAT_EACH_BREAK
				|| candidate->ad_->ad_unit()->take_all_chances_
#if defined(ADS_ENABLE_FORECAST)
				|| ctx->request_flags() & Ads_Request::Smart_Rep::BYPASS_BUDGET_CHECK
#endif
				|| (candidate->ad_->is_sponsorship() && candidate->ad_->repeat_mode_ == Ads_Advertisement::REPEAT_DEFAULT))
			{
				repeat_candidates.push_back(candidate);
				candidate->used_slots_.insert(slot);
			}

			/// frequency capping
			ctx->fc_->inc_occurences(candidate, slot, ctx);

			profit += candidate->effective_eCPM();
		}

		if (ctx->root_asset_ && !ctx->request_->rep()->capabilities_.disable_ad_unit_in_multiple_slots_)
		{
			for (Ads_Advertisement_Candidate_List::iterator it = repeat_candidates.begin(); it != repeat_candidates.end(); ++it)
			{
				Ads_Advertisement_Candidate *candidate = *it;

				std::list<Ads_Advertisement_Candidate_Ref *> refs;

				for (Ads_Slot_List::iterator sit = candidate->lite_->applicable_slots_.begin(); sit != candidate->lite_->applicable_slots_.end(); ++sit)
				{
					Ads_Slot_Base *slot = *sit;
					if (!slot || !ctx->is_slot_availiable_for_replicate_ad(*slot))
						continue;

					if (try_replicate_candidate_advertisement(ctx, candidate, slot, refs, profit) > 0)
					{
						candidate_combine_with_recover.reactivate();
						ctx->decision_info_.set_flag3(Decision_Info::INFO_3__FLAG_21);
					}
				}

					for (Ads_Advertisement_Candidate_Ref_List::iterator it = refs.begin(); it != refs.end(); ++it)
					{
						Ads_Advertisement_Candidate_Ref *ref = *it;
#if defined(ADS_ENABLE_FORECAST)
						this->take_advertisement_candidate_reference(ctx, ref, ctx->delivered_candidates_, slot_position, candidate_rank);
#else
						this->take_advertisement_candidate_reference(ctx, ref, ctx->delivered_candidates_);
#endif
					}
			}
		}
	}
#if defined(ADS_ENABLE_FORECAST)
	if (slot_position) delete slot_position;
	if (candidate_rank) delete candidate_rank;
#endif

	return 0;
}

int
Ads_Selector::knapsack_first_improve(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate_List& candidates, const Ads_Slot_List& slots, Ads_Advertisement_Candidate_Set& filters, int64_t& profit)
{
	bool verbose = (ctx->verbose_ > 0);
	size_t n_candidates = candidates.size();

	ADS_ASSERT(n_candidates > 0);

	for (size_t j = 0; j < n_candidates; ++j)
	{
		if (!candidates[j]->cast_video_slot()) continue;
		if (!candidates[j]->replacable(candidates[j]->slot_)) continue;
		if (candidates[j]->is_proposal()) continue;

		for (size_t k = j+1; k < n_candidates; ++k)
		{
			/// same slot
			if (!candidates[k]->cast_video_slot()) continue;
			if (!candidates[k]->replacable(candidates[k]->slot_)) continue;
			if (candidates[k]->is_proposal()) continue;

			if (candidates[j]->cast_video_slot() == candidates[k]->cast_video_slot()) continue;

///XXX: slot restriction
			if (!this->test_advertisement_slot_restrictions(ctx, candidates[j], candidates[k]->cast_video_slot(), 0, verbose)
			        || !this->test_advertisement_slot_restrictions(ctx, candidates[k], candidates[j]->cast_video_slot(), 0, verbose)
			   )
				continue;
			if (!candidates[j]->test_advertisement_exclusivity(ctx, filters, candidates[k]->cast_video_slot())
				|| !candidates[k]->test_advertisement_exclusivity(ctx, filters, candidates[j]->cast_video_slot()))
				continue;

			int delta_j = 0, delta_k = 0;
			if (ctx->use_actual_duration_for_improve_)
			{
				delta_j = candidates[j]->creative_duration(candidates[j]->slot()) - candidates[k]->duration(candidates[j]->slot());
				delta_k = candidates[k]->creative_duration(candidates[k]->slot()) - candidates[j]->duration(candidates[k]->slot());
			}
			else
			{
				delta_j = candidates[j]->duration(candidates[j]->slot()) - candidates[k]->duration(candidates[j]->slot());
				delta_k = candidates[k]->duration(candidates[k]->slot()) - candidates[j]->duration(candidates[k]->slot());
			}

			int duration_remain_j = int(candidates[j]->cast_video_slot()->duration_remain());
			int duration_remain_k = int(candidates[k]->cast_video_slot()->duration_remain());

			bool can_benefit_slot_j = delta_j > 0 && (duration_remain_k < 0 || (delta_k + duration_remain_k) >= 0);
			bool can_benefit_slot_k = delta_k > 0 && (duration_remain_j < 0 || (delta_j + duration_remain_j) >= 0);

			size_t h = j, l = k;
			if (!can_benefit_slot_j && !can_benefit_slot_k) continue;
			if ((can_benefit_slot_j && can_benefit_slot_k && delta_k > delta_j) || (can_benefit_slot_k && !can_benefit_slot_j))
			{
				std::swap(h, l);
			}
			int delta = (h == j) ? delta_j : delta_k;

			if (candidates[h]->cast_video_slot()->is_full(delta))
				continue;

			ADS_DEBUG((LP_DEBUG, "index_j %ld, %s:%s, max_duration %d, duration %d, creative_duration %d, delta_j %d, duration_remain_j %d, can_benefit_slot_j %s\n",
			           j,
			           ADS_ENTITY_ID_CSTR(candidates[j]->ad_id()),
			           candidates[j]->slot()->custom_id_.c_str(),
			           candidates[j]->cast_video_slot()->max_duration_,
			           candidates[j]->duration(candidates[j]->slot()),
			           candidates[j]->creative_duration(candidates[j]->slot()),
			           delta_j, duration_remain_j,
			           can_benefit_slot_j ? "true" : "false"));
			ADS_DEBUG((LP_DEBUG, "index_k %ld, %s:%s, max_duration %d, duration %d, creative_duration %d, delta_k %d, duration_remain_k %d, can_benefit_slot_k %s\n",
			           k,
			           ADS_ENTITY_ID_CSTR(candidates[k]->ad_id()),
			           candidates[k]->slot()->custom_id_.c_str(),
			           candidates[k]->cast_video_slot()->max_duration_,
			           candidates[k]->duration(candidates[k]->slot()),
			           candidates[k]->creative_duration(candidates[k]->slot()),
			           delta_k, duration_remain_k,
			           can_benefit_slot_k ? "true" : "false"));
			{
				size_t t = (size_t)-1;
				size_t duration = (size_t) -1;
				/// save masks
				uint64_t masks = candidates[h]->cast_video_slot()->masks_;
				/// pre mask for test_advertisement_slot_restrictions
				candidates[h]->cast_video_slot()->take_advertisement(ctx, candidates[l], true/*mask_only*/);
				for (size_t u=0; u < n_candidates; ++u)
				{
					if (!candidates[u]->should_swapped_in()) continue;

///XXX: slot restrictions & exclusivity
					if (!this->test_advertisement_slot_restrictions(ctx, candidates[u], candidates[h]->slot(), 0, verbose))
						continue;

					if (!this->test_advertisement_unit_sponsorship(candidates[u]->lite_, candidates[h]->slot(), verbose))
						continue;

					if (!ctx->fc_->pass_frequency_cap_by_slot(ctx, candidates[u])) continue;

					if (!ctx->fc_->pass_frequency_cap_time_based(ctx, candidates[u], candidates[h]->slot())) continue;

					if (!candidates[h]->cast_video_slot()->has_available_space(candidates[u]->duration(candidates[h]->cast_video_slot()), delta))
							continue;

					// similar fix as ESC-1121 for 'first improve', review it after hotfix
					// candidates[u] && candidates[l] are going to be put into same slot
					// So we made temporal slot assignment for candidates[l] to enable TARGET_AD_UNIT exclusivity checking
					Ads_Slot_Base *tmp = candidates[l]->slot();
					candidates[l]->slot_ = candidates[h]->slot();
					if (!candidates[u]->test_advertisement_exclusivity(ctx, filters, candidates[h]->slot()))
					{
						candidates[l]->slot_ = tmp;
						continue;
					}
					candidates[l]->slot_ = tmp;

					if (duration == (size_t)-1 || duration < candidates[u]->duration(candidates[h]->slot()))
					{
						duration = candidates[u]->duration(candidates[h]->slot());
						t = u;
					}
				}

				if (t != (size_t)-1)
				{
					candidates[l]->cast_video_slot()->remove_advertisement(ctx, candidates[l]);
					candidates[l]->cast_video_slot()->take_advertisement(ctx, candidates[h]);
					candidates[h]->cast_video_slot()->remove_advertisement(ctx, candidates[h]);
					candidates[h]->cast_video_slot()->take_advertisement(ctx, candidates[l]);
					candidates[h]->cast_video_slot()->take_advertisement(ctx, candidates[t]);

					candidates[t]->slot_ = candidates[h]->slot_;
					candidates[h]->slot_ = candidates[l]->slot_;
					candidates[l]->slot_ = candidates[t]->slot_;

					candidates[h]->flags_ |= Ads_Advertisement_Candidate::FLAG_SWAPPED;
					candidates[l]->flags_ |= Ads_Advertisement_Candidate::FLAG_SWAPPED;
					/// frequency capping
					ctx->fc_->inc_occurences(candidates[t], candidates[t]->slot_, ctx);

					profit += candidates[t]->effective_eCPM();
					filters.insert(candidates[t]);
				}
				else
					/// restore masks if failed
					candidates[h]->cast_video_slot()->masks_ = masks;
			}
		}
	}

	return 0;
}

int
Ads_Selector::knapsack_second_improve(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate_List& candidates, const Ads_Slot_List& slots, Ads_Advertisement_Candidate_Set& filters, int64_t& profit)
{
	bool verbose = (ctx->verbose_ > 0);

	size_t n_candidates = candidates.size();

	ADS_ASSERT(n_candidates > 0);

	for (size_t j1 = n_candidates; j1 > 0; --j1)
	{
		size_t j = j1-1;
		if (!candidates[j]->cast_video_slot()) continue;
		if (candidates[j]->is_proposal()) continue;

		///check if the ad can be exchanged
		if (!candidates[j]->replacable(candidates[j]->slot_))	continue;
		if (candidates[j]->is_companion())	continue;

		size_t duration = candidates[j]->cast_video_slot()->duration_remain() + candidates[j]->creative_duration(candidates[j]->slot());
		size_t n_advertisements =  candidates[j]->cast_video_slot()->num_advertisements_  - 1;

		filters.erase(candidates[j]);

		std::list<size_t> alternatives;
		int64_t alternative_profit = 0;
		int64_t competing_profit = 0;

		bool alternative_guaranteed = false;

		size_t alternative_duration = 0;

		/// save masks
		uint64_t masks = candidates[j]->cast_video_slot()->masks_;
		for (size_t k = 0; k < n_candidates; ++k)
		{
			if (!candidates[k]->should_swapped_in()) continue;

			if (!candidates[k]->replacable(candidates[j]->slot_)) continue;

			if (Ads_Selector::greater_advertisement_candidate(ctx, ads::MEDIA_VIDEO, candidates[j], candidates[k], true, true))
				continue;

			if (candidates[k]->duration(candidates[j]->slot()) <= duration)
			{
				if (n_advertisements >= candidates[j]->cast_video_slot()->max_num_advertisements_)
					continue;

				///XXX: slot restrictions & exclusivity
				if (!this->test_advertisement_slot_restrictions(ctx, candidates[k], candidates[j]->slot(), 0, verbose))
					continue;

				if (!this->test_advertisement_unit_sponsorship(candidates[k]->lite_, candidates[j]->slot(), verbose))
					continue;

				if (!ctx->fc_->pass_frequency_cap_by_slot(ctx, candidates[k])) continue;

				if (!ctx->fc_->pass_frequency_cap_time_based(ctx, candidates[k], candidates[j]->slot_)) continue;

				if (!candidates[k]->test_advertisement_exclusivity(ctx, filters, candidates[j]->slot()))
					continue;

				alternative_duration += candidates[k]->duration(candidates[j]->slot());

				duration -= candidates[k]->duration(candidates[j]->slot());
				++n_advertisements;

				// pre mask for test_advertisement_slot_restrictions
				candidates[j]->cast_video_slot()->take_advertisement(ctx, candidates[k], true/*mask_only*/);

				// TODO: fix for ESC-1121 "TARGET_AD_UNIT exclusivity", review it after hotfix
				candidates[k]->slot_ = candidates[j]->slot();

				alternatives.push_back(k);
				alternative_profit += candidates[k]->effective_eCPM(candidates[j]->slot());
				competing_profit += candidates[k]->competing_eCPM(candidates[j]->slot());

				//if (candidates[k]->is_guaranteed(ads::MEDIA_VIDEO))
				if (candidates[k]->is_guaranteed(candidates[j]->slot()))
					alternative_guaranteed = true;

				filters.insert(candidates[k]);
			}
		}

		bool is_frequency_cap_satisfied = true;
		std::map<Ads_GUID, int> frequency_cap_count;
		for (auto idx : alternatives)
		{
			auto candidate = candidates[idx];

			if (candidate->has_frequency_cap(ctx) || candidate->has_universal_id_frequency_cap(ctx))
			{
				auto &cnt = frequency_cap_count[ctx->fc_->get_ad_or_deal_id(candidate->lite_)];
				cnt++;
				if (!ctx->fc_->pass_frequency_cap_by_slot(ctx, candidate, false, NULL, cnt) || !ctx->fc_->pass_frequency_cap_time_based(ctx, candidate, candidates[j]->slot()))
				{
					is_frequency_cap_satisfied = false;
					break;
				}
			}
		}

		if (is_frequency_cap_satisfied && (competing_profit > candidates[j]->competing_eCPM(candidates[j]->slot()))
			// (alternative_profit > candidates[j]->effective_eCPM(ads::MEDIA_VIDEO))
			//&& (!candidates[j]->is_guaranteed(ads::MEDIA_VIDEO) || alternative_guaranteed)
			&& (!candidates[j]->is_guaranteed(candidates[j]->slot()) || alternative_guaranteed)
		)
		{
			Ads_Slot_Base *aslot = candidates[j]->slot();
			candidates[j]->cast_video_slot()->remove_advertisement(ctx, candidates[j]);

			for (std::list<size_t>::iterator it = alternatives.begin(); it != alternatives.end(); ++it)
			{
				candidates[j]->cast_video_slot()->take_advertisement(ctx, candidates[*it]);
				candidates[*it]->slot_ = aslot;
				ctx->fc_->inc_occurences(candidates[*it], aslot, ctx);
			}

			candidates[j]->slot_ = 0;

			/// frequency capping
			ctx->fc_->dec_occurences(candidates[j], aslot, ctx);
			profit += (alternative_profit - candidates[j]->effective_eCPM(aslot));
		}
		else
		{
			filters.insert(candidates[j]);
			// restore masks if failed
			candidates[j]->cast_video_slot()->masks_ = masks;

			for (std::list<size_t>::iterator it = alternatives.begin(); it != alternatives.end(); ++it)
			{
				candidates[*it]->slot_ = 0;
				filters.erase(candidates[*it]);
			}
		}
	}

	return 0;
}

int
Ads_Selector::knapsack_third_improve(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate_List& ads, const Ads_Slot_List& slots, Ads_Advertisement_Candidate_Set& filters, int64_t& profit)
{
	Ads_Advertisement_Candidate_List candidates(ads);
	std::stable_sort(candidates.begin(), candidates.end(), Ads_Advertisement_Candidate::greater(ctx, ads::MEDIA_VIDEO, true, true));

	/// reset valid2 flag
	for (Ads_Advertisement_Candidate_List::const_iterator cit = candidates.begin(); cit != candidates.end(); ++cit)
		(*cit)->valid2_ = true;

	std::set <Ads_Advertisement_Candidate *> processed_candidates;
	size_t n_candidates = candidates.size();

	for (size_t j = 0; j < n_candidates; ++j)
	{
		Ads_Advertisement_Candidate *candidate_j = candidates[j];

		if (!candidate_j->active(ads::MEDIA_VIDEO))
		{
			if (candidate_j->slot())
				processed_candidates.insert(candidate_j);
			continue;
		}

		if (candidate_j->is_proposal()) continue;

		// ignore leader + follower
		if (candidate_j->ad_->placement_ && (candidate_j->ad_->placement_->is_leader() || candidate_j->ad_->placement_->is_follower()))
			continue;

		// ignore very advanced link
		if (candidate_j->ad_->placement_ && (candidate_j->ad_->placement_->advanced_companion_ && candidate_j->ad_->placement_->companion_mode_ == Ads_Advertisement::NOT_LINKED))
			continue;

		if (candidate_j->is_pod_ad()) continue;

		if (!candidate_j->slot())
		{
			//size_t n_companion = candidate_j->companion_candidates_.size();

			if (!ctx->fc_->pass_frequency_cap_by_slot(ctx, candidates[j], true)) continue;
		}

		if (processed_candidates.find(candidate_j) != processed_candidates.end()) continue;

		Ads_Advertisement_Candidate_Vector new_candidates(candidate_j->companion_candidates_.begin(), candidate_j->companion_candidates_.end());
		if (new_candidates.empty()) new_candidates.push_back(candidate_j);

		for (size_t k = j + 1; k < n_candidates; ++k)
		{
			Ads_Advertisement_Candidate *candidate_k = candidates[k];

			if (!candidate_k->cast_video_slot() || candidate_k->cast_video_slot()->max_num_advertisements_ > 1) continue;
			if (!ctx->is_advertisement_compatible_with_slot(candidate_j->lite_, candidate_k->cast_video_slot(), false)) continue;
			if (candidate_k->is_proposal()) continue;

			if (!candidate_k->replacable(candidate_k->slot_))	continue;

			if (!candidate_k->valid_) continue;

			bool replacable = true;
			Ads_Advertisement_Candidate_List old_candidates;
			for (Ads_Advertisement_Candidate_Vector::iterator it = candidate_k->companion_candidates_.begin();
				 it != candidate_k->companion_candidates_.end(); ++it)
			{
				Ads_Advertisement_Candidate *current = *it;
				if (!current->slot_) continue;
//				if ((current != candidate_k && current->slot_->env() == ads::ENV_VIDEO) || !current->replacable(current->slot_))
                if (!current->replacable(current->slot_) || processed_candidates.find(current) != processed_candidates.end())
				{
					replacable = false;
					break;
				}

				old_candidates.push_back(current);
			}

			if (!replacable) continue;
			if (old_candidates.empty()) old_candidates.push_back(candidate_k);

			std::set<const Ads_Slot_Base *> candidate_slots;

			for (Ads_Advertisement_Candidate_List::const_iterator it = old_candidates.begin();
			        it != old_candidates.end();
			        ++it)
			{
				Ads_Advertisement_Candidate *candidate = *it;
				filters.erase(candidate);

				if (candidate->cast_video_slot())
				{
					candidate->cast_video_slot()->remove_advertisement(ctx, candidate);
					candidate_slots.insert(candidate->cast_video_slot());
				}
			}

			bool exchange = false;
			size_t n_companions = new_candidates.size();
			std::vector<Ads_Slot_Base *> plan(n_companions);
			std::multimap<Ads_Slot_Base *, Ads_Advertisement_Candidate *> occupied;

			size_t flags = 0;

			for (size_t i = 0; i < n_companions; ++i)
			{
				Ads_Advertisement_Candidate *candidate = new_candidates[i];
				Ads_Slot_Base *slot = 0;
				if (!candidate->valid_) continue;

				if (this->find_slot_for_advertisement(ctx, candidate, ctx->video_slots_, filters, occupied, slot, flags) >= 0 && slot)
				{
					plan[i] = slot;
					occupied.insert(std::make_pair(slot, candidate));

					if (ctx->request_->rep()->site_section_.video_player_.video_.auto_play_
						&& !ctx->keep_initial_ad()
						//&& reinterpret_cast<Ads_Video_Slot *>(slot)->time_position_ <= MAX_INITIAL_TIME_POSITION )
						&& !slot->force_first_call()
						&& int64_t(reinterpret_cast<Ads_Video_Slot *>(slot)->time_position_) <= ctx->max_initial_time_position_ )
						flags |= FLAG_CHECK_INITIAL;

					// if (candidate_slots.find(slot) !=  candidate_slots.end())
					if (slot == candidate_k->slot_)
						exchange = true;
				}
			}

			if (candidate_j->companion_candidates_.size() > 1) flags |= FLAG_CHECK_COMPANION;

			for (size_t i = 0; i < n_companions; ++i)
			{
				Ads_Advertisement_Candidate *candidate = new_candidates[i];

				if (!candidate->valid_) continue;
				if (plan[i]) continue;

				Ads_Slot_Base *slot = 0;
				if ((this->find_slot_for_advertisement(ctx, candidate, ctx->player_slots_, filters, occupied, slot, flags) >= 0 && slot)
				        || (this->find_slot_for_advertisement(ctx, candidate, ctx->page_slots_, filters, occupied, slot, flags) >= 0 && slot))
				{
					plan[i] = slot;
					occupied.insert(std::make_pair(slot, candidate));
				}
			}

			if (exchange && candidate_j->ad_->companion_)
			{
				if (!validate_advertisement_companion(ctx, new_candidates, plan))
				{
					/// unable to deliver the package
					ADS_DEBUG((LP_DEBUG, "advanced ALL LINKED companion package %s not able to deliver\n", ADS_ENTITY_ID_CSTR(candidate_j->ad_->id_)));
					exchange = false;
				}
			}

			if (!exchange)
			{
				for (Ads_Advertisement_Candidate_List::const_iterator it = old_candidates.begin();
				        it != old_candidates.end();
				        ++it)
				{
					Ads_Advertisement_Candidate *candidate = *it;

					filters.insert(candidate);
					if (candidate->cast_video_slot())
						candidate->cast_video_slot()->take_advertisement(ctx, candidate);
				}
			}
			else
			{
				for (Ads_Advertisement_Candidate_List::const_iterator it = old_candidates.begin();
				        it != old_candidates.end();
				        ++it)
				{
					Ads_Advertisement_Candidate *candidate = *it;

					if (candidate->slot_ && (candidate->slot_->env() == ads::ENV_PLAYER || candidate->slot_->env() == ads::ENV_PAGE))
					{
//J						ctx->used_slots_.erase(candidate->slot_);
						ctx->unuse_slot(candidate, candidate->slot_);
					}

					profit -= candidate->effective_eCPM();

					ctx->fc_->dec_occurences(candidate, candidate->slot_, ctx);
					candidate->slot_ = 0;
				}

				this->take_companion(ctx, candidate_j, new_candidates, filters, plan, flags, profit);
				processed_candidates.insert(new_candidates.begin(), new_candidates.end());
			}
		}
	}

	return 0;
}


int
Ads_Selector::find_advertisement_for_slot(Ads_Selection_Context *ctx, const Ads_Slot_Base *slot, const Ads_Advertisement_Candidate_List& candidates, const Ads_Advertisement_Candidate_Set& filters, Ads_Advertisement_Candidate*& selected_candidate, size_t flags, size_t max_tries /* = -1 */)
{
	selected_candidate = 0;

	bool verbose = (ctx->verbose_ > 0);

	if (!slot->has_profile())
	{
		for (Ads_Advertisement_Candidate_List::const_iterator it = candidates.begin(); it != candidates.end(); ++it)
		{
			Ads_Advertisement_Candidate *candidate = *it;
			if (candidate->lite_->is_watched()) ctx->log_watched_info(*candidate->lite_, slot, "NO_PROFILE_FOR_SLOT");
		}
		ADS_DEBUG((LP_ERROR, "slot %s has no profile.\n", slot->custom_id_.c_str()));
		return -1;
	}

	size_t count = 0;
	for (Ads_Advertisement_Candidate_List::const_iterator it = candidates.begin(); it != candidates.end(); ++it)
	{
		Ads_Advertisement_Candidate *candidate = *it;

		// not active
		if (!candidate->active(slot->associate_type()))
			continue;

#if defined(ADS_ENABLE_FORECAST)
		if ((flags & FLAG_CHECK_PROPOSAL_ONLY) && !candidate->is_proposal()) //TODO only used by forecast, deprecated
			continue;
#endif

		bool check_companion = true;
		if (ctx->request_->has_explicit_candidates()) check_companion = ctx->request_->rep()->ads_.check_companion_;

//		size_t n_companion = 1;
		if (check_companion && candidate->is_companion())
		{
			//if (!slot->accept_companion() &&
			//	!(candidate->flags_ & Ads_Advertisement_Candidate::FLAG_NO_TEMPORAL)
			//   )
			if (!(candidate->flags_ & Ads_Advertisement_Candidate::FLAG_NO_TEMPORAL))
			{
				if (candidate->lite_->is_watched()) ctx->log_watched_info(*candidate->lite_, slot, "NOT_ACCEPT_COMPANION");
				continue;
			}

//			n_companion = candidate->companion_candidates_.size();
		}

		//if (! ctx->fc_->test_advertisement_frequency_cap(ctx, candidate, n_companion, true/*exclude count freq exempt ad*/)) continue;
		if (!ctx->fc_->pass_frequency_cap_by_slot(ctx, candidate)) continue;

		if (!ctx->fc_->pass_frequency_cap_time_based(ctx, candidate, const_cast<Ads_Slot_Base*>(slot))) continue;

///XXX: slot restrictions & advertisement exclusivity
		if (!this->test_advertisement_slot_restrictions(ctx, candidate, slot, 0, verbose))
			continue;

		if (!this->test_advertisement_unit_sponsorship(candidate->lite_, slot, verbose))
			continue;
		if (!candidate->test_advertisement_exclusivity(ctx, filters, slot))
			continue;

		if (!this->test_advertisement_refresh_exclusivity(ctx, candidate, slot))
			continue;

		if (!selected_candidate
			|| Ads_Selector::greater_advertisement_candidate(ctx, slot->associate_type(), candidate, selected_candidate)
		   )
			selected_candidate = candidate;

		if (++ count >= max_tries)
		{
			ADS_DEBUG((LP_ERROR, "too many tries\n"));
			break;
		}
	}

	return 0;
}

int
Ads_Selector::is_slot_applicable_for_advertisement(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate *candidate, Ads_Slot_List &slots,
		const Ads_Advertisement_Candidate_Set &filters,
		const std::multimap<Ads_Slot_Base*, Ads_Advertisement_Candidate *> &occupied_slots, Ads_Slot_Base *slot, Ads_Slot_Base *&backup_slot,
		size_t flags, std::map<Ads_Slot_Base*, const Ads_Advertisement_Candidate *> *companion_leaders, std::vector<Ads_Slot_Base*> *vect_recoverable)
{
	if (ctx == nullptr || slot == nullptr) return -1;
	const Ads_Advertisement_Candidate_Lite *lite = candidate->lite_;
	Ads_Slot_Base *selected_slot = nullptr;

	bool verbose = (ctx->verbose_ > 0);
	do
	{
		if (!candidate->slot_ref(slot))
			continue;

		if (!slot->has_profile())
		{
			for (std::set<Ads_Advertisement_Candidate *>::const_iterator fit = filters.begin(); fit != filters.end(); ++fit)
			{
				Ads_Advertisement_Candidate *fcandidate = *fit;
				if (fcandidate->lite_->is_watched()) ctx->log_watched_info(*fcandidate->lite_, slot, "NO_PROFILE_FOR_SLOT");
			}
			continue;
		}

		// FDB-11398
		if (slot->filled_by_multi_ads_)
		{
			//ESC-34283
			//always true for ads, AF portfolio AD in UGA round will skip this check
			if (!candidate->ad_->is_portfolio())
			{
				lite->log_selection_error(selection::Error_Code::SLOT_FILLED_BY_MULTI_ADS, slot, selection::Error_Code::COMPETITION_FAILURE);
				ADS_DEBUG((LP_DEBUG, "ad %s failed for slot %s: slot already filled by multi-ads tag\n", CANDIDATE_LITE_ID_CSTR(lite), slot->custom_id_.c_str()));
				continue;
			}
		}

		// not active
		if (!candidate->active(slot->associate_type()))
			continue;

		if (flags & FLAG_CHECK_INITIAL)
		{
			if (!slot->accept_initial())
			{
				ADS_DEBUG((LP_DEBUG, "ad %s failed for slot %s: can not accept initial ads\n", CANDIDATE_LITE_ID_CSTR(lite), slot->custom_id_.c_str()));
				if (lite->is_watched()) ctx->log_watched_info(*lite, slot, "NOT_ACCEPT_INITIAL_ADS");
				continue;
			}

			if (candidate->is_companion() && ctx->is_slot_used(slot, Ads_Selection_Context::STANDALONE))
			{
				ADS_DEBUG((LP_DEBUG, "ad %s failed for slot %s: can not accept intial companions, already taken by standalone ad\n", CANDIDATE_LITE_ID_CSTR(lite), slot->custom_id_.c_str()));
				if (lite->is_watched()) ctx->log_watched_info(*lite, slot, "INITIAL_POSITON_TAKEN_BY_STANDALONE");
				continue;
			}
		}

		if (flags & FLAG_CHECK_STANDALONE)
		{
			if (!slot->accept_standalone() || ctx->is_slot_used(slot, Ads_Selection_Context::STANDALONE))
			{
				ADS_DEBUG((LP_DEBUG, "ad %s failed for slot %s: can not accept standalone ads\n", CANDIDATE_LITE_ID_CSTR(lite), slot->custom_id_.c_str()));
				if (lite->is_watched()) ctx->log_watched_info(*lite, slot, "NOT_ACCEPT_STANDALONE_ADS");
				continue;
			}

			if (ctx->is_slot_used(slot, Ads_Selection_Context::INITIAL_COMPANION))
			{
				ADS_DEBUG((LP_DEBUG, "ad %s failed for slot %s: can not accept initial ads\n", CANDIDATE_LITE_ID_CSTR(lite), slot->custom_id_.c_str()));
				if (lite->is_watched()) ctx->log_watched_info(*lite, slot, "NOT_ACCEPT_INITIAL_ADS");
				continue;
			}
		}

		if (flags & FLAG_CHECK_FALLBACK_INITIAL)
		{
			if (!slot->accept_initial() || ctx->is_slot_used(slot, Ads_Selection_Context::STANDALONE))
				continue;
		}

		if (flags & FLAG_CHECK_COMPANION)
		{
			bool check_companion = true;
			if (ctx->request_->has_explicit_candidates()) check_companion = ctx->request_->rep()->ads_.check_companion_;

			if (check_companion && candidate->is_companion() && !slot->accept_companion()
				&& !(candidate->flags_ & Ads_Advertisement_Candidate::FLAG_NO_TEMPORAL)
			   )
			{
				if (lite->is_watched()) ctx->log_watched_info(*lite, slot, "NOT_ACCEPT_COMPANION");
				continue;
			}
		}

		std::pair<const std::multimap<Ads_Slot_Base *, Ads_Advertisement_Candidate *>::const_iterator,
			const std::multimap<Ads_Slot_Base *, Ads_Advertisement_Candidate *>::const_iterator> sits = occupied_slots.equal_range(slot);

		//FDB-4042  Adserver Should be able to deliver multiple ad units from same placment into one slot when linked
		if (slot->env() == ads::ENV_VIDEO)
		{
			Ads_Video_Slot *vslot = reinterpret_cast<Ads_Video_Slot *>(slot);

			size_t num_advertisements = vslot->num_advertisements_;
			size_t duration_remain = vslot->duration_remain();
			//configured ad can't be delivered as dynamic in the same slot
			if (candidate->is_scheduled_in_slot(slot))
			{
				ADS_DEBUG((LP_DEBUG, "configured ad:%s for slot %s can't be delivered into same slot as dynamic ad\n",
					ADS_ENTITY_ID_CSTR(candidate->ad_->id_), slot->custom_id_.c_str()));
				continue;
			}
			// under uga/ga scenario, static mode is considered as blend mode
			bool is_force_proposal = ctx->request_flags() & Ads_Request::Smart_Rep::FORCE_PROPOSAL;
			if (!is_force_proposal && vslot->is_schedule_mode(Ads_Slot_Template::Slot_Info::STATIC_SCHEDULE))
			{
				ADS_DEBUG((LP_DEBUG, "vslot schedule mode is STATIC_SCHEDULE ad:%s, slot [%d]%s\n",
					ADS_ENTITY_ID_CSTR(candidate->ad_->id_), vslot->position_, slot->custom_id_.c_str()));
				continue;
			}

			for (std::multimap<Ads_Slot_Base *, Ads_Advertisement_Candidate *>::const_iterator sit = sits.first; sit != sits.second; ++sit)
			{
				const Ads_Advertisement_Candidate * current = sit->second;
				++num_advertisements;
				duration_remain -= current->duration(slot);
			}

			if (num_advertisements >= vslot->max_num_advertisements_)
			{
				lite->log_selection_error(selection::Error_Code::EXCEED_MAX_NUM_ADVERTISEMENTS, slot, selection::Error_Code::COMPETITION_FAILURE);
				if (lite->is_watched()) ctx->log_watched_info(*lite, slot, "MAX_NUM_ADVERTISEMENTS " + ctx->restriction_.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_UNIT, -1, true, slot));
				candidate->lite_->set_sub_reject_reason_bitmap(Reject_Ads::SLOT_MAX_NUM_AD);

				ctx->mkpl_executor_->log_ad_error(ctx, *lite, selection::Error_Code::EXCEED_MAX_NUM_ADVERTISEMENTS, selection::Error_Code::COMPETITION_FAILURE);
				ADS_DEBUG((LP_DEBUG, "meet max num advertisements ad:%s, slot %s\n", CANDIDATE_LITE_ID_CSTR(lite), slot->custom_id_.c_str()));
				continue;
			}

			if (duration_remain < candidate->duration(slot))
			{
				lite->log_selection_error(selection::Error_Code::EXCEED_MAX_SLOT_DURATION, slot, selection::Error_Code::COMPETITION_FAILURE);
				if (lite->is_watched()) ctx->log_watched_info(*lite, slot, "MAX_SLOT_DURATION " + ctx->restriction_.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_UNIT, -1, true, slot));
				candidate->lite_->set_sub_reject_reason_bitmap(Reject_Ads::SLOT_MAX_DURATION);

				ctx->mkpl_executor_->log_ad_error(ctx, *lite, selection::Error_Code::EXCEED_MAX_SLOT_DURATION, selection::Error_Code::COMPETITION_FAILURE);
				ADS_DEBUG((LP_DEBUG, "meet max slot duration ad:%s, slot %s\n", CANDIDATE_LITE_ID_CSTR(lite), slot->custom_id_.c_str()));
				continue;
			}
		}
		else
		{
			// slot occupied
			if (sits.first != sits.second)
			{
				if (candidate != 0 && candidate->ad_ != 0 && slot != 0)
				{
					ADS_DEBUG((LP_DEBUG, "ad %s failed for slot %s: the slot is occupied, need to search others slots \n", CANDIDATE_LITE_ID_CSTR(lite), slot->custom_id_.c_str()));
				}
				continue;
			}
		}

///XXX: slot restrictions & advertisement exclusivity
		bool recoverable = false;
		if (!this->test_advertisement_slot_restrictions(ctx, candidate, slot, 0, verbose, recoverable))
		{
			if (vect_recoverable != NULL && recoverable)
				vect_recoverable->push_back(slot);
			continue;
		}

		if (!this->test_advertisement_unit_sponsorship(lite, slot, verbose))
		{
			candidate->lite_->set_sub_reject_reason_bitmap(Reject_Ads::SLOT_SPONSORSHIP);
			continue;
		}

		Ad_Excluded_Reason reason;
		if (!candidate->test_advertisement_exclusivity(ctx, filters, slot, &reason))
		{
			selection::Error_Code error_code = reason.error_code(selection::Error_Code::EXCLUSIVITY_BY_SLOT);
			lite->log_selection_error(error_code, slot, selection::Error_Code::COMPETITION_FAILURE);

			ctx->mkpl_executor_->log_ad_error(ctx, *lite, selection::Error_Code::EXCLUSIVITY_BY_SLOT, selection::Error_Code::COMPETITION_FAILURE);

			uint32_t reject_reason = 0;
			if (ctx->decision_info_.reject_ads_.get_reject_reason(error_code, reject_reason) > 0)
				candidate->lite_->set_sub_reject_reason_bitmap(reject_reason);
			else
				candidate->lite_->set_sub_reject_reason_bitmap(Reject_Ads::SLOT_EXCLUSIVITY);

			if (lite->is_pre_selection_external_translated() && reason.stream_excluded_)//only early return for fake ad for safe reason
			{
				return -1;
			}

			continue;
		}

		if (!ctx->fc_->pass_frequency_cap_time_based(ctx, candidate, slot, &occupied_slots))
		{
			candidate->lite_->set_sub_reject_reason_bitmap(Reject_Ads::SLOT_TIMED_FREQ_CAP);
			continue;
		}

		if (candidate->ad_->is_follower())
		{
			const Ads_Advertisement_Candidate *leader = 0;

			if (this->find_leader_for_follower(ctx, candidate, leader, slots, slot, companion_leaders) < 0 || !leader)
			{
				ADS_DEBUG((LP_DEBUG, "no leader found for follower %s, slot %s\n", ADS_ENTITY_ID_CSTR(candidate->ad_->id_), slot->custom_id_.c_str()));
				if (lite->is_watched()) ctx->log_watched_info(*lite, slot, "NO_LEADER_FOUND");
				continue;
			}
		}

		// FIXME: tricky way to get a more suitable slot for companion leader
		if (candidate->ad_->is_leader() && candidate->is_companion())
		{
			Ads_Video_Slot* vslot = reinterpret_cast<Ads_Video_Slot *>(slot);
			const Ads_Slot_Base *previous_slot = (vslot->parent_ ? vslot->parent_ : vslot);

			/// check following slot untill next interruptive slot
			bool found_other_follower = false;

				Ads_Slot_List::iterator it = std::find(slots.begin(), slots.end(), slot);
				ADS_ASSERT(it != slots.end());
				++it;

				std::multimap<Ads_Video_Slot *, Ads_Advertisement_Candidate *> removed_candidates;
				for (; it < slots.end(); ++it)
				{
					Ads_Slot_Base *aslot = *it;
					Ads_Video_Slot* vslot = reinterpret_cast<Ads_Video_Slot *>(aslot);

					if (vslot->time_position_class_ == "overlay" || vslot->time_position_class_ == "pause_midroll")
					{
						if (vslot->advertisements_.empty()) continue;

						for (Ads_Advertisement_Candidate_Set::const_iterator ait = vslot->advertisements_.begin();
							 ait != vslot->advertisements_.end(); ++ait)
						{
							Ads_Advertisement_Candidate *filled_candidate = *ait;
							const Ads_Advertisement *filled_ad = filled_candidate->ad_;

							if (filled_ad->placement_id() != candidate->ad_->placement_id())
							{
								// FIXME: ad hoc fix for ABC
								if (candidate->ad_->ad_unit() && candidate->ad_->ad_unit()->position_in_slot_ == -1 && candidate->ad_->is_leader())
								{
									if (filled_ad->is_follower() && (
												!filled_ad->companion_ || filled_ad->companion_when_possible_ ||
												(filled_ad->advanced_companion_  && (filled_ad->ad_linking_groups_.empty() || filled_ad->ad_linking_groups_[0].second != Ads_Advertisement::ALL_LINKED))))
									{
										removed_candidates.insert(std::make_pair(vslot, filled_candidate));
										continue;
									}
								}

								ADS_DEBUG((LP_DEBUG, "leader ad(%s)'s succeeding slot %s contains other follower ad(%s)\n"
										   , ADS_ENTITY_ID_CSTR(candidate->ad_->id_), vslot->custom_id_.c_str(), ADS_ENTITY_ID_CSTR(filled_ad->id_)));
								found_other_follower = true;
								break;
							}
						}

						if (found_other_follower) break;
					}
					else if (vslot != previous_slot && vslot->parent_ != previous_slot)
						break;
				}

			if (!found_other_follower)
			{
				for (std::multimap<Ads_Video_Slot *, Ads_Advertisement_Candidate *>::iterator it = removed_candidates.begin();
					 it != removed_candidates.end(); ++it)
				{
					Ads_Video_Slot *rslot = it->first;
					Ads_Advertisement_Candidate *rcandidate = it->second;

					ADS_DEBUG((LP_DEBUG, "swap out follower(%s) from slot(%s) because of previous last in slot leader\n"
							   , ADS_ENTITY_ID_CSTR(rcandidate->ad_->id_), rslot->custom_id_.c_str()));

					rslot->remove_advertisement(ctx, rcandidate);
					rcandidate->non_applicable_slots_.insert(rslot);
				}

				selected_slot = slot;
				break;
			}
			else if (!backup_slot)
				backup_slot = slot;

			continue;
		}

		selected_slot = slot;
		break;
	} while (false);

	return selected_slot == slot ? 0 : -2;
}

int
Ads_Selector::find_slot_for_advertisement(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate* candidate, Ads_Slot_List& slots,
		const Ads_Advertisement_Candidate_Set& filters,
		const std::multimap<Ads_Slot_Base *, Ads_Advertisement_Candidate *>& occupied_slots, Ads_Slot_Base *& selected_slot,
		size_t flags, std::map<Ads_Slot_Base *, const Ads_Advertisement_Candidate *>* companion_leaders, std::vector<Ads_Slot_Base*>* vect_recoverable)
{
	if (ctx == nullptr) return -1;
	const Ads_Advertisement_Candidate_Lite *lite = candidate->lite_;
	selected_slot = 0;

	bool verbose = (ctx->verbose_ > 0);

	Ads_Slot_Base *backup_slot = 0;

	for (Ads_Slot_List::const_iterator it = slots.begin(); it != slots.end(); ++it)
	{
		Ads_Slot_Base *slot = *it;
		int ret = is_slot_applicable_for_advertisement(ctx, candidate, slots, filters, occupied_slots, slot, backup_slot, flags, companion_leaders, vect_recoverable);
		if (ret == -1)
			return -1;
		if (ret == 0)
		{
			selected_slot = slot;
			break;
		}
	}

	if (selected_slot == nullptr && backup_slot != nullptr)
		selected_slot = backup_slot;

	if (selected_slot == nullptr && lite->external_ad_info().selection_errors_.empty())
		candidate->lite_->set_error(selection::Error_Code::NO_SLOT_SELECTED);

	return 0;
}

///FIXME: inefficient implementation
bool
Ads_Selector::has_valid_slot_for_advertisement(Ads_Selection_Context* ctx, Ads_Advertisement_Candidate *candidate, Ads_Slot_Base*& aslot, Ads_Slot_Set& occupied)
{
	if (!candidate->scheduled_slots_.empty())//always true for airing configured ads.
	{
		aslot = const_cast<Ads_Slot_Base*>(*(candidate->scheduled_slots_.begin()));
		return true;
	}

	for (Ads_Slot_List::const_iterator it = ctx->video_slots_.begin(); it != ctx->video_slots_.end(); ++it)
	{
		Ads_Slot_Base *slot = *it;
		if (occupied.find(slot) != occupied.end() && slot->max_num_advertisements_ == 1) continue;

		if (this->test_advertisement_slot_restrictions(ctx, candidate, slot, FLAG_IGNORE_ALL, false))
		{
			aslot = slot;
			return true;
		}
	}

	for (size_t j = 0; j < ctx->player_slots_.size(); ++j)
	{
		Ads_Slot_Base *slot = ctx->player_slots_[j];
		if (occupied.find(slot) != occupied.end()) continue;

		if (this->test_advertisement_slot_restrictions(ctx, candidate, slot, FLAG_IGNORE_ALL, false))
		{
			aslot = slot;
			return true;
		}
	}

	for (size_t j = 0; j < ctx->page_slots_.size(); ++j)
	{
		Ads_Slot_Base *slot = ctx->page_slots_[j];
		if (occupied.find(slot) != occupied.end())
		{
			if (candidate != 0 && candidate->ad_ != 0 && slot != 0)
			{
				ADS_DEBUG((LP_DEBUG, "slot validating, ad: %s, find valid slot, but occupied, skip slot %s\n", ADS_ENTITY_ID_CSTR(candidate->ad_->id_), slot->custom_id_.c_str()));
			}
			continue;
		}

		if (this->test_advertisement_slot_restrictions(ctx, candidate, slot, FLAG_IGNORE_ALL, false))
		{
			aslot = slot;
			return true;
		}
	}

	return false;
}

bool
Ads_Selector::try_companion(Ads_Selection_Context* ctx, Ads_GUID placement_id, Ads_GUID companion_id, Ads_Advertisement_Candidate *candidate, std::map<Ads_Advertisement_Candidate *, Ads_GUID> &parents, std::map<Ads_GUID, Ads_Slot_Set> &valid_occupied, bool is_custom_set, std::map<Ads_GUID, Ads_Advertisement_Candidate_List> &custom_set_candidates)
{
	parents.insert(std::make_pair(candidate, placement_id));
	Ads_Slot_Set &occupied = valid_occupied[companion_id];
	Ads_Slot_Base *slot = 0;
	if (!this->has_valid_slot_for_advertisement(ctx, candidate, slot, occupied) || !slot)
	{
		if (candidate->ad_ != 0)
		{
			ADS_DEBUG((LP_DEBUG, "companion validating, ad: %s, no valid slot, eliminated from companion package, %s\n", ADS_ENTITY_ID_CSTR(candidate->ad_->id_), ADS_ENTITY_ID_CSTR(companion_id)));
		}
		if (!is_custom_set)
		{
			candidate->valid_ = false;
			if (candidate->lite_->is_watched() && candidate->ad_ != NULL) ctx->log_watched_info(*candidate->lite_, "NO_VALID_SLOT");
		}
		return false;
	}
	if (candidate->non_applicable_slots_.find(slot) == candidate->non_applicable_slots_.end())
	{
		if (candidate != 0 && candidate->ad_ != 0 && slot != 0)
		{
			ADS_DEBUG((LP_DEBUG, "companion validating, ad: %s, find valid slot take slot,(companion_id)%s: (slot)%s\n", ADS_ENTITY_ID_CSTR(candidate->ad_->id_), ADS_ENTITY_ID_CSTR(companion_id),
				slot->custom_id_.c_str()));
		}
		occupied.insert(slot);
	}
	if (is_custom_set)
	{
		Ads_Advertisement_Candidate_List &custom_set_ads = custom_set_candidates[companion_id];
		if (candidate->ad_->is_leader())
			custom_set_ads.push_front(candidate);
		else
			custom_set_ads.push_back(candidate);
	}
	return true;
}
bool
Ads_Selector::custom_set_cmp(const std::pair<Ads_GUID, std::pair<int64_t, int64_t> > &a, const std::pair<Ads_GUID, std::pair<int64_t, int64_t> > &b)
{
	//custom set with higher (v)price is in front.
	return a.second.first > b.second.first;
}

bool compare_dimension_greater(const Ads_Advertisement_Candidate* left, const Ads_Advertisement_Candidate* right)
{
	if (left == 0 || left->ad_unit() == 0 || left->ad_ == 0 || left->ad_unit()->is_temporal())
	{
		if (right == 0 || right->ad_unit() == 0 || right->ad_ == 0 || right->ad_unit()->is_temporal())
			return false;
		return true;
	}
	if (right == 0 || right->ad_unit() == 0 || right->ad_ == 0 || right->ad_unit()->is_temporal())
		return false;

	return left->ad_->width() * left->ad_->height() > right->ad_->width() * right->ad_->height();
}

//
//class Custom_Set_Greater : public std::greater<std::pair<Ads_GUID, std::pair<int64_t, int64_t> > >
//{
//	const std::map<Ads_GUID, Ads_Advertisement_Candidate_List>& custom_set_candidates_;
//public:
//	Custom_Set_Greater(const std::map<Ads_GUID, Ads_Advertisement_Candidate_List>& custom_set):custom_set_candidates_(custom_set)
//	{
//
//	}
//
//	bool operator()(const std::pair<Ads_GUID, std::pair<int64_t, int64_t> > &left, const std::pair<Ads_GUID, std::pair<int64_t, int64_t> > &right) const
//	{
//		if (left.second.first != right.second.first)
//			return left.second.first > right.second.first;
//
//		//1.get candidates for each item, and sort it by dim
//		std::map<Ads_GUID, Ads_Advertisement_Candidate_List>::const_iterator it;
//		it = custom_set_candidates_.find(left.first);
//		if (it == custom_set_candidates_.end())
//			return false;
//		Ads_Advertisement_Candidate_List left_candidates = it->second;
//
//		it = custom_set_candidates_.find(right.first);
//		if (it == custom_set_candidates_.end())
//			return true;
//		Ads_Advertisement_Candidate_List right_candidates = it->second;
//		stable_sort(left_candidates.begin(),  left_candidates.end(), compare_dimension_greater);
//		stable_sort(right_candidates.begin(), right_candidates.end(), compare_dimension_greater);
//
//
//		//2.compare the dim value, return true if left > right
//		Ads_Advertisement_Candidate_List::iterator right_it = right_candidates.begin();
//		Ads_Advertisement_Candidate_List::iterator left_it = left_candidates.begin();
//		while(right_it != right_candidates.end())
//		{
//			Ads_Advertisement_Candidate* value = *right_it;
//			if (value == 0 || value->ad_unit() == 0 || !value->ad_unit()->is_temporal())
//				break;
//			right_it++;
//		}
//		while(left_it != left_candidates.end())
//		{
//			Ads_Advertisement_Candidate* value = *left_it;
//			if (value == 0 || value->ad_unit() == 0 || !value->ad_unit()->is_temporal())
//				break;
//			left_it++;
//		}
//
//		for (;
//			left_it != left_candidates.end(); left_it++)
//		{
//			Ads_Advertisement_Candidate* left_value = *left_it;
//			if (left_value == 0 || left_value->ad_unit() == 0)
//				return false;
//			if (right_it == right_candidates.end())
//				return true;
//			Ads_Advertisement_Candidate* right_value = *right_it;
//			if (right_value == 0 || right_value->ad_unit() == 0)
//				return true;
//
//			size_t left_dim = left_value->ad_->width() * left_value->ad_->height();
//			size_t right_dim = right_value->ad_->width() * right_value->ad_->height();
//			if (left_dim != right_dim)
//				return left_dim > right_dim;
//			right_it++;
//		}
//		return false;
//	}
//};



void
Ads_Selector::prepare_for_companion_validation(Ads_Selection_Context* ctx, Ads_Advertisement_Candidate_List& candidates)
{
	ADS_ASSERT(ctx && ctx->repository_);
	typedef std::map<Ads_GUID, std::pair<uint64_t, uint64_t> > Companion_Prices;

	std::map<Ads_Advertisement_Candidate *, Ads_GUID> parents;
	Companion_Prices prices;
	std::map<Ads_GUID, uint64_t> internal_prices;
	std::map<Ads_GUID, Ads_Advertisement_Candidate_List> children;
	std::map<Ads_GUID, Ads_Slot_Set> valid_occupied;
	std::map<Ads_GUID, bool> has_temporal_ad;

	Companion_Prices custom_set_prices;
	std::map<Ads_Advertisement_Candidate *, Ads_GUID_Set> ad_in_companions;
	std::map<Ads_GUID, Ads_Advertisement_Candidate_List> custom_set_candidates;
	Ads_Advertisement_Candidate_List copy_list(candidates);

	stable_sort(copy_list.begin(),  copy_list.end(), compare_dimension_greater);

	//this block check every ads, if it has an valid slot, then insert the ad into companion(children and custom_set_candidates);
	//children:std::map<Ads_GUID, Ads_Advertisement_Candidate_List>:the key is placement id ,the value is all valid ads in this placement
	//custom_set_candidates: std::map<Ads_GUID, Ads_Advertisement_Candidate_List>, for very advance link only, the key is custom_set id,
	//the value is all valid ads in this custom_set.
	//ad_in_companions: map<Ads_Advertisement_Candidate *, Ads_GUID_Set>, for very advance link only,  key is ad, the value is the custom_id set which includes the key ad.
	for (Ads_Advertisement_Candidate_List::iterator it = copy_list.begin(); it != copy_list.end(); ++it)
	{
		Ads_Advertisement_Candidate *candidate = *it;
		if (!candidate->valid_) continue;
		if (candidate->is_follower_pod_ad()) continue;

		const Ads_Advertisement *ad = candidate->ad_;
		ADS_ASSERT(ad);
		if (!ad->companion_) continue;

		const Ads_Advertisement *placement = 0;
		if (!ad->is_old_external())
		{
			Ads_GUID placement_id = ad->placement_id();
			if (ctx->find_advertisement(placement_id, placement) < 0 || !placement) continue;
		}
		// external or not very advanced link companion
		if (ad->is_old_external() || (placement && !(placement->advanced_companion_ && placement->companion_mode_ == Ads_Advertisement::NOT_LINKED)))
		{
			Ads_GUID companion_id = ad->is_old_external() ? ad->companion_id_ : ad->placement_id();
			if (candidate->ad_unit()->is_temporal()) has_temporal_ad[companion_id] = true;
			if (try_companion(ctx, companion_id, companion_id, candidate, parents, valid_occupied, false/*is custom set*/, custom_set_candidates))
			{
				Ads_Advertisement_Candidate_List &ads = children[companion_id];
				if (candidate->ad_->is_leader())
					ads.push_front(candidate);
				else
					ads.push_back(candidate);
			}
		}
		else//very advanced link
		{
			Ads_GUID placement_id = ad->placement_id();
			Ads_GUID_Set& companion_ids = ad_in_companions[candidate];
			if (ad->ad_linking_groups_.empty())
			{
				if (try_companion(ctx, placement_id, ad->id_, candidate, parents, valid_occupied, true/*is custom set*/, custom_set_candidates))
					companion_ids.insert(ad->id_);
			}
			else
			{
				for (Ads_Advertisement::Ad_Linking_Group_RVector::const_iterator ait = ad->ad_linking_groups_.begin(); ait != ad->ad_linking_groups_.end(); ++ait)
				{
					Ads_GUID companion_id = ait->first;
					if (try_companion(ctx, placement_id, companion_id, candidate, parents, valid_occupied, true/*is custom set*/, custom_set_candidates))
						companion_ids.insert(companion_id);
				}
			}
			//add candidate to children no matter try companion succeed
			Ads_Advertisement_Candidate_List &ads = children[placement_id];
			if (candidate->ad_->is_leader())
				ads.push_front(candidate);
			else
				ads.push_back(candidate);
		}
	}

	//do some basic validation:
	//1.for external tag's companion, it should be all-link companion,check whether all ads in companion are valid, otherwise, set the companion as invalid;
	//2.for very advance link companion, if an ad is not in any custom_set, set the ad as invalid.
	//3.if a placement has any temporal ad, and the companion has no valid temporal slot,then the companion is not valid
	//4.for advance companion whose link mode is not not-link, if any ad in custom_set is not valid, then mark the custom_set as invalid
	for (std::map<Ads_GUID, Ads_Advertisement_Candidate_List>::iterator it=children.begin(); it != children.end(); ++it)
	{//early escape validateion
		Ads_Advertisement_Candidate_List &companion_candidates = it->second;
		const size_t n = companion_candidates.size();
		bool satisfied = true;

		if (companion_candidates.empty()) continue;

		Ads_Advertisement_Candidate *candidate = companion_candidates[0];
		ADS_ASSERT(candidate && candidate->ad_);

		if (candidate->ad_->is_old_external())
		{	//external ads can only be ALL-LINKED
			satisfied =  (companion_candidates.size() == candidate->ad_->companion_count_);
		}
		else
		{	//normal ads companion
			std::map<Ads_GUID, size_t > n_ads_in_group;
			std::map<Ads_GUID, std::vector<Ads_GUID> > ads_in_group;

			bool has_valid_temporal_slot = false;

			Ads_GUID placement_id = candidate->ad_->placement_id();
			const Ads_Advertisement *placement = 0;
			if (ctx->find_advertisement(placement_id, placement) < 0 || !placement) continue;

			//for very advanced link placement, if an ad does not have valid slot in any custom set, mark it as invalid
			if (placement->advanced_companion_ && placement->companion_mode_ == Ads_Advertisement::NOT_LINKED)
			{
				for (size_t i = 0; i < n; ++i)
				{
					Ads_Advertisement_Candidate *candidate = companion_candidates[i];
					if (ad_in_companions[candidate].size() == 0)
					{
						candidate->valid_ = false;
						if (candidate->lite_->is_watched()) ctx->log_watched_info(*candidate->lite_, "NO_VALID_SLOT");
					}
				}
			}

			size_t n_children = 0;
			for (Ads_GUID_RSet::const_iterator it = placement->children_.begin(); it != placement->children_.end(); ++it)
			{
				const Ads_Advertisement *child = 0;
				if (ctx->find_advertisement(*it, child) < 0 || !child
					|| !child->is_active(ctx->time(), ctx->enable_testing_placement())
					|| !child->companion_ || !child->ad_unit())
					continue;

				for (Ads_Advertisement::Ad_Linking_Group_RVector::const_iterator iit = child->ad_linking_groups_.begin(); iit != child->ad_linking_groups_.end(); ++iit)
				{
					n_ads_in_group[iit->first]++;
					ads_in_group[iit->first].push_back(child->id_);
				}

				if (child->ad_unit()->is_temporal())
					has_temporal_ad[placement_id] = true;

				++n_children;
			}

			//preparation for validation
			size_t n_valid_slots = 0;
			for (size_t i = 0; i < n; ++i)
			{
				Ads_Advertisement_Candidate *candidate = companion_candidates[i];

				if (!candidate->valid_) continue;

				if (candidate->ad_unit()->is_temporal())
				{
					has_valid_temporal_slot = true;
				}

				for (Ads_Advertisement::Ad_Linking_Group_RVector::const_iterator cit = candidate->ad_->ad_linking_groups_.begin(); cit != candidate->ad_->ad_linking_groups_.end(); ++cit)
					n_ads_in_group[cit->first]--;

				++n_valid_slots;
			}

			do
			{//validation
				std::set<Ads_GUID> invalid_groups;
				std::set<Ads_GUID> valid_groups;

				if (!has_valid_temporal_slot && has_temporal_ad[placement_id]) //early escape
				{
					ADS_DEBUG((LP_DEBUG, "phase1 check: companion package: %s no temproral ads(required) delivered\n", ADS_ENTITY_ID_CSTR(placement_id)));
					satisfied = false;
					break;
				}

				if (!placement->advanced_companion_ && placement->companion_mode_ == Ads_Advertisement::ALL_LINKED)
				{
					satisfied = (n_children == n_valid_slots);

					if (!satisfied)
					{
						ADS_DEBUG((LP_DEBUG, "phase1 check: companion package: %s all-linked, not all children filled\n", ADS_ENTITY_ID_CSTR(placement_id)));
						break;
					}
				}

				if (placement->advanced_companion_ && placement->companion_mode_ != Ads_Advertisement::NOT_LINKED)
				{	//advanced companion by ad_unit type, LIMITATION: currently one ad can only appears in one group.
					//bool check = false;
					//check every group whose linking type is ALL_LINKED
					{
						for (Ads_Advertisement::Ad_Linking_Group_RVector::const_iterator it = placement->ad_linking_groups_.begin(); it != placement->ad_linking_groups_.end(); ++it)
						{
							if (invalid_groups.find(it->first) != invalid_groups.end()) continue;

							if (it->second == Ads_Advertisement::ALL_LINKED && n_ads_in_group[it->first] != 0)
							{
								invalid_groups.insert(it->first);
								//check = true;

								//if group1(ad1,ad2) ALL-LINKED and group2(ad2,ad3) ALL-LINKED, then group2's faliure will result to group1's faliure
								std::list<Ads_GUID> invalid_group_ids;
								invalid_group_ids.push_back(it->first);
								while(!invalid_group_ids.empty())
								{
									Ads_GUID group_id = invalid_group_ids.front();
									invalid_group_ids.pop_front();

									std::vector<Ads_GUID>  &ads_vec = ads_in_group[group_id];
									for (std::vector<Ads_GUID>::iterator iit = ads_vec.begin(); iit != ads_vec.end(); ++iit)
									{
										const Ads_Advertisement *ad = 0;
										if (ctx->find_advertisement(*iit, ad) < 0 || !ad) continue;

										for (Ads_Advertisement::Ad_Linking_Group_RVector::const_iterator ait = ad->ad_linking_groups_.begin(); ait != ad->ad_linking_groups_.end(); ++ait)
										{
											if (ait->second == Ads_Advertisement::ALL_LINKED && invalid_groups.find(ait->first) == invalid_groups.end())
											{
												invalid_groups.insert(ait->first);
												invalid_group_ids.push_back(ait->first);
											}
										}
									}
								}
							}
						}
					}

					//if (check) //re-check
					{
						//eliminate candiates whose group is invalid
						has_valid_temporal_slot = false;
						for (size_t i = 0; i < n; ++i)
						{
							Ads_Advertisement_Candidate *candidate = companion_candidates[i];
							if (!candidate->valid_) continue;

							for (Ads_Advertisement::Ad_Linking_Group_RVector::const_iterator cit = candidate->ad_->ad_linking_groups_.begin(); cit != candidate->ad_->ad_linking_groups_.end(); ++cit)
							{
								if (invalid_groups.find(cit->first) != invalid_groups.end())
								{
									candidate->valid_ = false;
									if (candidate->lite_->is_watched()) ctx->log_watched_info(*candidate->lite_, "COMPANION_NO_INVENTORY");
									ADS_DEBUG((LP_DEBUG, "phase1 check: companion package: %s COMPANION_NO_INVENTORY\n", CANDIDATE_LITE_ID_CSTR(candidate->lite_)));
									break;
								}
							}

							if (candidate->valid_)
							{
								if (candidate->ad_unit()->is_temporal())
									has_valid_temporal_slot = true;

								for (Ads_Advertisement::Ad_Linking_Group_RVector::const_iterator cit = candidate->ad_->ad_linking_groups_.begin(); cit != candidate->ad_->ad_linking_groups_.end(); ++cit)
									valid_groups.insert(cit->first);
							}
						}
					}

					if (!has_valid_temporal_slot && has_temporal_ad[placement_id])
					{
						ADS_DEBUG((LP_DEBUG, "phase1 check: (advanced link) companion package: %s with no video ads can not be delivered\n", ADS_ENTITY_ID_CSTR(placement_id)));
						satisfied = false;
						break;
					}

					if (placement->companion_mode_ == Ads_Advertisement::ALL_LINKED)
					{
						for (Ads_Advertisement::Ad_Linking_Group_RVector::const_iterator it = placement->ad_linking_groups_.begin(); it != placement->ad_linking_groups_.end(); ++it)
						{
							if (ads_in_group[it->first].empty()) continue;
							if (invalid_groups.find(it->first) != invalid_groups.end() || valid_groups.find(it->first) == valid_groups.end())
							{
								ADS_DEBUG((LP_DEBUG, "phase1 check: advanced all-link companion %s fail due to  set %s not able to serve\n", ADS_ENTITY_ID_CSTR(placement_id), ADS_ENTITY_ID_CSTR(it->first)));
								satisfied = false;
								break;
							}
						}
					}
				}
			}
			while (0);
		}

		if (!satisfied)
		{//phase1 validataion fail
			for (Ads_Advertisement_Candidate_List::iterator it = companion_candidates.begin(); it != companion_candidates.end(); ++it)
			{
				(*it)->valid_ = false;
				if ((*it)->lite_ != nullptr)
					ctx->decision_info_.reject_ads_.put(*((*it)->lite_), Reject_Ads::PREP_COMPANION);
				if ((*it)->lite_->is_watched()) ctx->log_watched_info(*(*it)->lite_, "COMPANION_NO_INVENTORY");
				ADS_DEBUG((LP_DEBUG, "phase1 check: companion package:(ad) %s COMPANION_NO_INVENTORY\n", CANDIDATE_LITE_LPID_CSTR(candidate->lite_)));
			}

			companion_candidates.clear();
		}
	}

	for (auto cit = children.begin(); cit != children.end(); ++cit)
	{
		Ads_Advertisement_Candidate_List &companion_candidates = cit->second;
		if (companion_candidates.empty()) continue;

		Ads_Advertisement_Candidate_List display_ads;
		for (auto it = companion_candidates.begin(); it != companion_candidates.end(); /*NULL*/)
		{
			Ads_Advertisement_Candidate *candidate = *it;
			if (!candidate->valid_)
			{
				it = companion_candidates.erase(it);
				continue;
			}

			if (!candidate->ad_->budget_exempt_)
			{
				uint64_t vprice = (candidate->is_guaranteed(ads::MEDIA_VIDEO, true) && candidate->ad_->competing_eCPM_ >= 0
						? candidate->ad_->competing_eCPM_
						: std::max(candidate->effective_eCPM(ads::MEDIA_VIDEO), candidate->effective_eCPM(ads::MEDIA_SITE_SECTION)));
				uint64_t sprice = (candidate->is_guaranteed(ads::MEDIA_SITE_SECTION, true) && candidate->ad_->competing_eCPM_ >= 0
						? candidate->ad_->competing_eCPM_
						: candidate->effective_eCPM(ads::MEDIA_SITE_SECTION));

				uint64_t internal_price = (candidate->is_guaranteed() && candidate->ad_->competing_eCPM_ >= 0
							? candidate->ad_->competing_eCPM_
							: candidate->internal_effective_eCPM());

				prices[cit->first].first += vprice;
				prices[cit->first].second += sprice;
				internal_prices[cit->first] += internal_price;

				for (const auto &g : candidate->ad_->ad_linking_groups_)
				{
					Ads_GUID key = ads::entity::make_id(1, g.first);
					prices[key].first += vprice;
					prices[key].second += sprice;
					internal_prices[key] += internal_price;
				}

				for (Ads_GUID companion_id : ad_in_companions[candidate])
				{
					custom_set_prices[companion_id].first += vprice;
					custom_set_prices[companion_id].second += sprice;
				}
			}

			//put display ads to the tail
			if (!candidate->ad_unit()->is_temporal())
			{
				display_ads.push_back(candidate);
				it = companion_candidates.erase(it);
				continue;
			}

			++it;
		}

		companion_candidates.insert(companion_candidates.end(), display_ads.begin(), display_ads.end());
	}

	//build placement custom sets
	for (std::map<Ads_GUID, Ads_Advertisement_Candidate_List>::iterator it=children.begin(); it != children.end(); ++it)
	{
		Ads_Advertisement_Candidate_List &companion_candidates = it->second;
		if (companion_candidates.empty()) continue;
		Ads_Advertisement_Candidate *first_candidate = companion_candidates[0];
		const size_t n = companion_candidates.size();
		Ads_GUID placement_id = first_candidate->ad_->placement_id();
		const Ads_Advertisement *placement = 0;
		if (ctx->find_advertisement(placement_id, placement) < 0 || !placement) continue;
		//very advanced link
		if (placement->advanced_companion_ && placement->companion_mode_ == Ads_Advertisement::NOT_LINKED)
		{
			Ads_GUID_Set processed_custom_sets;
			//sort custom set by prices
			std::vector<std::pair<Ads_GUID, std::pair<int64_t, int64_t> > > custom_set_price_vector;
			for (size_t i = 0; i < n; i++)
			{
				Ads_Advertisement_Candidate *candidate = companion_candidates[i];
				for (Ads_GUID_Set::iterator sit = ad_in_companions[candidate].begin(); sit != ad_in_companions[candidate].end(); ++sit)
				{
					if (processed_custom_sets.find(*sit) != processed_custom_sets.end()) continue;
					std::pair<Ads_GUID, std::pair<int64_t, int64_t> > custom_set_price_pair = std::make_pair(*sit, custom_set_prices[*sit]);
					custom_set_price_vector.push_back(custom_set_price_pair);
					processed_custom_sets.insert(*sit);
				}
			}
			//Custom_Set_Greater greater(custom_set_candidates);
			std::stable_sort(custom_set_price_vector.begin(), custom_set_price_vector.end(), custom_set_cmp);
			//put sorted custom sets into ctx->placement_custom_sets_
			Linking_Set_List_Vector& placement_custom_sets = ctx->placement_custom_sets_[placement_id];
			for (std::vector<std::pair<Ads_GUID, std::pair<int64_t, int64_t> > >::iterator cit = custom_set_price_vector.begin(); cit != custom_set_price_vector.end(); ++cit)
			{
				Linking_Set_Pair p = std::make_pair(cit->first, custom_set_candidates[cit->first]);
				placement_custom_sets.push_back(p);
			}
		}
	}

	for (Ads_Advertisement_Candidate_List::const_iterator it = copy_list.begin(); it != copy_list.end(); ++it)
	{
		Ads_Advertisement_Candidate *candidate = *it;

		if (!candidate->valid_) continue;

		if (parents.find(candidate) == parents.end()) continue;
		Ads_GUID parent = parents[candidate];

		const Ads_Advertisement *placement = 0;
		if (ctx->find_advertisement(parent, placement) >= 0 && placement
		   && placement->advanced_companion_ && placement->companion_mode_ == Ads_Advertisement::NOT_LINKED)
		{
			if (!candidate->ad_->ad_linking_groups_.empty())
			{
				Ads_GUID key = ads::entity::make_id(1, candidate->ad_->ad_linking_groups_.begin()->first);
				candidate->companion_effective_eCPM(ads::MEDIA_VIDEO) = prices[key].first;
				candidate->companion_effective_eCPM(ads::MEDIA_SITE_SECTION) = prices[key].second;

				candidate->internal_companion_effective_eCPM_ = internal_prices[key];
			}
		}
		else
		{
			candidate->companion_effective_eCPM(ads::MEDIA_VIDEO) = prices[parent].first;
			candidate->companion_effective_eCPM(ads::MEDIA_SITE_SECTION) = prices[parent].second;

			candidate->internal_companion_effective_eCPM_ = internal_prices[parent];
		}

		if (!has_temporal_ad[parent]) candidate->flags_ |= Ads_Advertisement_Candidate::FLAG_NO_TEMPORAL;
		candidate->companion_candidates_.assign(children[parent].begin(), children[parent].end());
	}

	return;
}


bool
Ads_Selector::validate_advertisement_companion(Ads_Selection_Context* ctx, Ads_Advertisement_Candidate_Vector &companion_candidates, std::vector<Ads_Slot_Base *> &plan, Ads_GUID custom_set_id)
{
	ADS_ASSERT(ctx && ctx->repository_);
	ADS_ASSERT(companion_candidates.size() == plan.size());

	size_t n_applicable_slots = 0;
	const size_t n = companion_candidates.size();
	bool satisfied = true;

	if (companion_candidates.empty()) return false;

	Ads_Advertisement_Candidate *candidate = companion_candidates[0];
	ADS_ASSERT(candidate && candidate->ad_);

	for (size_t i = 0; i < n; ++i)
	{
		if (plan[i]) ++n_applicable_slots;
	}

	if (candidate->ad_->is_old_external())
	{	//external ads can only be ALL-LINKED
		if (n_applicable_slots == candidate->ad_->companion_count_)
			return true;
		else
		{
			ADS_DEBUG((LP_DEBUG, "phase2 check: ad %s failed in companion test,companion count not meet\n", ADS_ENTITY_ID_CSTR(candidate->ad_->id_)));
			return false;
		}
	}

	{	//normal ads companion
		std::map<Ads_GUID, bool> display_group;
		std::map<Ads_GUID, std::vector<Ads_GUID> > ads_in_group;

		Ads_GUID placement_id = candidate->ad_->placement_id();
		const Ads_Advertisement *placement = 0;
		if (ctx->find_advertisement(placement_id, placement) < 0 || !placement) return false;

		for (Ads_GUID_RSet::const_iterator it = placement->children_.begin(); it != placement->children_.end(); ++it)
		{
			const Ads_Advertisement *child = 0;
			if (ctx->find_advertisement(*it, child) < 0 || !child
				|| !child->is_active(ctx->time(), ctx->enable_testing_placement())
				|| !child->companion_ || !child->ad_unit())
				continue;

			for (Ads_Advertisement::Ad_Linking_Group_RVector::const_iterator iit = child->ad_linking_groups_.begin(); iit != child->ad_linking_groups_.end(); ++iit)
			{
				ads_in_group[iit->first].push_back(child->id_);
				if (!child->ad_unit()->is_temporal()) display_group[iit->first] =true;
			}
		}

		//validation
		//if (satisfied)
		{
			Ads_GUID_Set  has_applicable_slots, pending_followers;
			Ads_GUID_Set  has_valid_slots;
			bool has_leader = false;
			for (size_t i = 0; i < n; ++i)
			{
				Ads_Advertisement_Candidate *candidate = companion_candidates[i];
				if (!candidate->valid_) continue;

				has_valid_slots.insert(candidate->ad_->id_);
				if (plan[i])
				{
					has_applicable_slots.insert(candidate->ad_->id_);
					if (candidate->ad_->is_leader()) has_leader = true;
				}
				else
				{//FIXME: FDB-5806: ignore follower if no applicable slot
					if (candidate->ad_->is_follower() && (placement->companion_when_possible_  ||
													      (placement->advanced_companion_  &&
														    (candidate->ad_->ad_linking_groups_.empty() || candidate->ad_->ad_linking_groups_[0].second != Ads_Advertisement::ALL_LINKED))
														))
						pending_followers.insert(candidate->ad_->id_);
				}

				if (has_leader) has_applicable_slots.insert(pending_followers.begin(), pending_followers.end());
			}

			if (placement->advanced_companion_ && placement->companion_mode_ == Ads_Advertisement::NOT_LINKED)
			{
				bool valid = true;
				if (!custom_set_id) return false;
				std::vector<Ads_GUID> &ads = ads_in_group[custom_set_id];
				Ads_Advertisement::COMPANION_MODE companion_mode = Ads_Advertisement::NOT_LINKED;
				for (Ads_Advertisement::Ad_Linking_Group_RVector::const_iterator cit = placement->ad_linking_groups_.begin(); cit != placement->ad_linking_groups_.end(); ++cit)
				{
					if (cit->first == custom_set_id)
					{
						companion_mode = cit->second;
						break;
					}
				}
				valid = (companion_mode == Ads_Advertisement::NOT_LINKED) ? valid :
					(companion_mode == Ads_Advertisement::ALL_LINKED) ? has_applicable_slots.size() == ads.size() :
					has_applicable_slots.size() == has_valid_slots.size();
				if (!valid)
				{
					ADS_DEBUG((LP_DEBUG, "phase2 check: companion %s not able to serve\n", ADS_ENTITY_ID_CSTR(custom_set_id)));
					satisfied = false;
				}
			}
			else
			{
				if (placement->advanced_companion_)
				{
					bool valid_video_group = false;
					for (Ads_Advertisement::Ad_Linking_Group_RVector::const_iterator cit = placement->ad_linking_groups_.begin(); cit != placement->ad_linking_groups_.end(); ++cit)
					{
						std::vector<Ads_GUID> &ads = ads_in_group[cit->first];
						size_t n_valid_ads = 0;
						size_t n_failed_ads = 0;

						for (std::vector<Ads_GUID>::const_iterator it = ads.begin(); it != ads.end(); ++it)
						{
							if (has_applicable_slots.find(*it) == has_applicable_slots.end() && has_valid_slots.find(*it) != has_valid_slots.end())
							{
								++n_failed_ads;
							}else if (has_valid_slots.find(*it) != has_valid_slots.end())
								++n_valid_ads;
						}

						if ((!ads.empty() && n_failed_ads == ads.size() && !display_group[cit->first]) || (cit->second != Ads_Advertisement::NOT_LINKED && n_failed_ads > 0) || (!ads.empty() && n_valid_ads == 0 && placement->companion_mode_ == Ads_Advertisement::ALL_LINKED))
						{
							ADS_DEBUG((LP_DEBUG, "phase2 check: advanced companion %s not able to serve due to set %s not able to serve\n", ADS_ENTITY_ID_CSTR(placement_id), ADS_ENTITY_ID_CSTR(cit->first)));
							satisfied = false;
							break;
						}
						else if (!display_group[cit->first] && !ads.empty() && n_valid_ads > 0) valid_video_group = true;
					}

					if (!valid_video_group)//no video ad can deliver
					{
						ADS_DEBUG((LP_DEBUG, "phase2 check: advanced companion %s not able to serve due to no video ads can deliver\n", ADS_ENTITY_ID_CSTR(placement_id)));
						satisfied = false;
					}
				}
				else if (has_applicable_slots.size() != has_valid_slots.size())
				{
					ADS_DEBUG((LP_DEBUG, "phase2 check: companion %s not able to serve\n", ADS_ENTITY_ID_CSTR(placement_id)));
					satisfied = false;
				}
			}
		}

		if (!satisfied && candidate->lite_->is_watched())
		{
			ctx->log_watched_info(*candidate->lite_, "COMPANION");
		}
	}

	return satisfied;
}

int
Ads_Selector::merge_advertising_slots_terms(Ads_Selection_Context* ctx, Ads_Slot_List& slots)
{
	size_t n_slots = slots.size();
	for (size_t i = 0; i < n_slots; ++i)
	{
		Ads_Slot_Base* slot = slots[i];

		if (slot->env() == ads::ENV_VIDEO)
		{
			Ads_Video_Slot* vslot = reinterpret_cast<Ads_Video_Slot *>(slot);

			if (!vslot) continue;

			if (!vslot->time_position_class_.empty() && !(vslot->flags_ & Ads_Slot_Base::FLAG_TRACKING_ONLY))
			{
				Ads_String slot_desc = vslot->time_position_class_ + "/0";
				Ads_GUID slot_term = ctx->repository_->slot_term(slot_desc.c_str());
				if (ads::entity::is_valid_id(slot_term))
				{
					vslot->slot_terms().insert(slot_term);
				}
				else
				{
					ADS_DEBUG((LP_ERROR, "slot term %s not found\n", slot_desc.c_str()));
				}
			}
		}
		else
		{
			Ads_String slot_desc = "display/0";
			Ads_GUID slot_term = ctx->repository_->slot_term(slot_desc.c_str());
			if (ads::entity::is_valid_id(slot_term))
			{
				slot->slot_terms().insert(slot_term);
			}
			else
			{
				ADS_DEBUG((LP_ERROR, "slot term %s not found\n", slot_desc.c_str()));
			}
		}

		const Ads_Environment_Profile *profile = slot->profile_;
		ADS_ASSERT(profile);

		if (profile->type_ == Ads_Environment_Profile::NORMAL)
		{
			if (!profile->slot_terms_.empty())
			{
				if (!ads::set_overlapped(
				            slot->slot_terms().begin(),
				            slot->slot_terms().end(),
				            profile->slot_terms_.begin(),
				            profile->slot_terms_.end())
				   )
				{
					ADS_DEBUG((LP_INFO, "slot type for profile %s not match on slot %s\n", profile->name_.c_str(), slot->custom_id_.c_str()));
					continue;
				}
			}
			slot->load_profile(profile);
		}
		else
		{
			ADS_ASSERT(profile->type_ == Ads_Environment_Profile::COMPOUND);
			for (Ads_Environment_Profile::Profile_Set::const_iterator it = profile->sub_profiles_.begin();
			        it != profile->sub_profiles_.end();
			        ++it)
			{
				const Ads_Environment_Profile *aprofile = *it;
				ADS_ASSERT(aprofile->type_ == Ads_Environment_Profile::NORMAL);
				if (aprofile->type_ != Ads_Environment_Profile::NORMAL) continue;

				if (!aprofile->slot_terms_.empty())
				{
					if (!ads::set_overlapped(
					            slot->slot_terms().begin(),
					            slot->slot_terms().end(),
					            aprofile->slot_terms_.begin(),
					            aprofile->slot_terms_.end())
					   )
					{
						ADS_DEBUG((LP_INFO, "slot type for profile %s not match on slot %s\n", profile->name_.c_str(), slot->custom_id_.c_str()));
						continue;
					}
				}
				slot->load_profile(aprofile);
			}
		}

		if (!ctx->profile_)
			slot->merge();
	}

	for (size_t i = 0; i < n_slots; ++i)
	{
		Ads_Slot_Base* slot = slots[i];
		if (!slot->has_profile())
		{
			ADS_DEBUG((LP_ERROR, "slot %s has no profile. no ad could be delivered.\n", slot->custom_id_.c_str()));
		}
	}

	return 0;
}

int
Ads_Selector::fill_advertising_slots_p(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate_List& candidates, Ads_Slot_List& slots, Ads_Advertisement_Candidate_Set& filters, int64_t& profit)
{
	profit = 0;
	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG((LP_TRACE, "filling %d slots\n", slots.size()));
		for (size_t i = 0; i < slots.size(); ++i)
		{
			Ads_Slot_Base *slot = slots[i];

			ADS_DEBUG0((LP_TRACE, "    slot %d: %s, max ads %d, indicators %x, ad unit restrictions %s, dimension %d x %d\n",
			            i,
			            slot->custom_id_.c_str(),
			            slot->max_num_advertisements_,
			            slot->indicators_,
						ads::entities_to_str(slot->restriction_ids_).c_str(),
						slot->width_,
						slot->height_)
			          );
		}
		ADS_DEBUG0((LP_TRACE, "\n"));
	}

	if (candidates.empty())
	{
		ADS_DEBUG((LP_DEBUG, "no candidate.\n"));
		return 0;
	}

	size_t n_slots = slots.size();
	for (size_t i = 0; i < n_slots; ++i)
	{
		Ads_Slot_Base *aslot = slots[i];
		if (!aslot->has_profile())
		{
			for (Ads_Advertisement_Candidate_List::const_iterator it = candidates.begin(); it != candidates.end(); ++it)
			{
				Ads_Advertisement_Candidate *candidate = *it;
				if (candidate->lite_->is_watched()) ctx->log_watched_info(*candidate->lite_, aslot, "NO_PROFILE_FOR_SLOT");
			}
			continue;
		}

		//if (! aslot->accept_initial())
		if (!aslot->accept_standalone())
		{
			ADS_DEBUG((LP_ERROR, "slot %s, can not accept standalone ads\n", aslot->custom_id_.c_str()));
			aslot->load_order_ = "v";
			if (!ctx->watched_candidates_.empty())
			{
				for (Ads_GUID_Set::const_iterator ait = ctx->watched_candidates_.begin();
					 ait != ctx->watched_candidates_.end();
					 ++ait)
				{
					const Ads_Advertisement *ad = nullptr;
					if (ctx->find_advertisement(*ait, ad) >= 0 && ad != nullptr && ad->ad_unit() && ctx->is_advertisement_compatible_with_slot(ad, aslot, ctx->get_watched_id(*ad)))
						ctx->log_watched_info(*ad, aslot, "NOT_ACCEPT_STANDALONE_ADS");
				}
			}
			continue;
		}

		if (aslot->no_standalone_if_temporal() && (ctx->status_ & STATUS_HAS_TEMPORAL_AD))
			continue;

		size_t flags = 0;
//J		if (ctx->used_slots_.find(aslot) != ctx->used_slots_.end())
		if (ctx->is_slot_used(aslot))
		{
#if !defined(ADS_ENABLE_FORECAST)
			continue;
#endif
		}

		// loop until finds a candidate or no candidate could be found
		aslot->load_order_ = "v";
		while (true)
		{
#if !defined(ADS_ENABLE_FORECAST)
			/// 1. a candidate is found, out
			if (flags) break;
#else
//J			if (ctx->used_slots_.find(aslot) != ctx->used_slots_.end())
			if (ctx->is_slot_used(aslot))
				break;
#endif

			Ads_Advertisement_Candidate *candidate = 0;
			if (this->find_advertisement_for_slot(ctx, aslot, candidates, filters, candidate, flags) < 0)
			{
				ADS_ASSERT(0);
				break;
			}

			// clear display refresh history and try again
			if (!candidate && ctx->request_->rep()->capabilities_.refresh_display_slots_
				&& (ctx->refresh_config_ || ctx->request_->rep()->capabilities_.refresh_display_on_demand_))
			{
				if (!ctx->user_->shrink_display_refresh_history_to_last_ad(aslot->custom_id_))
					break;

				ADS_DEBUG((LP_DEBUG, "clear display refresh history for slot %s and try again\n", aslot->custom_id_.c_str()));
				if (this->find_advertisement_for_slot(ctx, aslot, candidates, filters, candidate, flags) < 0)
				{
					ADS_ASSERT(0);
					break;
				}

				// remove last deliverd ad
				// this candidate will be added to the list later in Phase 10
				if (candidate)
				{
					ctx->user_->clear_display_refresh_history(aslot->custom_id_);
				}
			}

			/// 2. no more candidates, out
			if (!candidate) break;

			bool check_companion = true;
			if (ctx->request_->has_explicit_candidates()) check_companion = ctx->request_->rep()->ads_.check_companion_;

			if (!check_companion || !candidate->is_companion())
			{
				candidate->slot_ = aslot;

				filters.insert(candidate);
					candidate->slot_->load_order_ = "i";

//J					ctx->used_slots_.insert(aslot);
					ctx->use_slot(candidate, aslot, Ads_Selection_Context::STANDALONE);

				if (!candidate->is_proposal())
				{
					/// mark filled
					flags = FLAG_CHECK_PROPOSAL_ONLY; //TODO only used by forecast, deprecated
				}

				/// frequency capping
				ctx->fc_->inc_occurences(candidate, candidate->slot_, ctx);

				profit += candidate->effective_eCPM();
			}
			else
			{
				size_t n_companions = candidate->companion_candidates_.size();

				if (!ctx->fc_->pass_frequency_cap_by_slot(ctx, candidate, true)/* check companion */)
				{
					for (size_t i = 0; i < n_companions; ++i)
						candidate->companion_candidates_[i]->valid2_ = false;
					continue;
				}

				std::vector<Ads_Slot_Base *> plan(n_companions);
				std::multimap<Ads_Slot_Base *, Ads_Advertisement_Candidate *> occupied;

				bool satisfied = true;
//J				occupied.insert(ctx->used_slots_.begin(), ctx->used_slots_.end());
				for (std::map<const Ads_Slot_Base *, int>::const_iterator it = ctx->slot_uses_.begin();
					it != ctx->slot_uses_.end();
					++it)
				{
					const Ads_Slot_Base *slot = it->first;
					if (!slot->no_initial_if_companion() && !(it->second & Ads_Selection_Context::INITIAL)) continue;
					occupied.insert(std::make_pair(const_cast<Ads_Slot_Base *>(slot), (Ads_Advertisement_Candidate *)0));
				}

				occupied.insert(std::make_pair(aslot, candidate));

				for (size_t i = 0; i < n_companions; ++i)
				{
					Ads_Advertisement_Candidate *current = candidate->companion_candidates_[i];
					if (current == candidate)
					{
						plan[i] = aslot;
						continue;
					}

					plan[i] = 0;
					Ads_Slot_Base *pslot = 0;
					if (aslot->env() == ads::ENV_PLAYER)
					{
						if (this->find_slot_for_advertisement(ctx, current, ctx->player_slots_, filters, occupied, pslot, FLAG_CHECK_COMPANION | FLAG_CHECK_STANDALONE) >= 0 && pslot)
						{
							plan[i] = pslot;
							occupied.insert(std::make_pair(pslot, current));
							continue;
						}
					}

					pslot = 0;
					if (this->find_slot_for_advertisement(ctx, current, ctx->page_slots_, filters, occupied, pslot, FLAG_CHECK_COMPANION | FLAG_CHECK_STANDALONE) >= 0 && pslot)
					{
						plan[i] = pslot;
						occupied.insert(std::make_pair(pslot, current));
						continue;
					}

					/// unable to deliver the package
					ADS_DEBUG((LP_DEBUG, "all display companion package %s not able to deliver due to ad %s failure of deliver\n", ADS_ENTITY_ID_CSTR(candidate->ad_->placement_id()), CANDIDATE_LITE_ID_CSTR(candidate->lite_)));
					satisfied = false;
					break;
				}

				if (!satisfied)
				{
					for (size_t i = 0; i < n_companions; ++i)
						candidate->companion_candidates_[i]->valid2_ = false;
				}
				else
				{
					this->take_companion(ctx, candidate, candidate->companion_candidates_, filters, plan, flags, profit);
					/// mark filled
					for (size_t i = 0; i < n_companions; ++i)
					{
						Ads_Advertisement_Candidate *current = candidate->companion_candidates_[i];
						if (!current->is_proposal())
						{
							flags = FLAG_CHECK_PROPOSAL_ONLY; //TODO only used by forecast, deprecated
							break;
						}
					}
				}
			}
		}
	}
	return 0;
}

int
Ads_Selector::fill_advertising_slots_v(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate_List& candidates, Ads_Slot_List& slots, Ads_Advertisement_Candidate_Set& filters, int64_t& profit)
{
	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG((LP_TRACE, "filling %d slots\n", slots.size()));
		for (size_t i = 0; i < slots.size(); ++i)
		{
			Ads_Slot_Base *slot = slots[i];
			ADS_ASSERT(slot->env() == ads::ENV_VIDEO);

			ADS_DEBUG0((LP_TRACE, "    slot %d: %s, max duration %d, max ads %d, max ad duration %d, ad unit restrictions %s\n",
			            i,
			            slot->custom_id_.c_str(),
			            reinterpret_cast<Ads_Video_Slot *>(slot)->max_duration_,
			            reinterpret_cast<Ads_Video_Slot *>(slot)->max_num_advertisements_,
			            reinterpret_cast<Ads_Video_Slot *>(slot)->max_advertisement_duration_,
					 	ads::entities_to_str(slot->restriction_ids_).c_str())
			          );
		}
		ADS_DEBUG0((LP_TRACE, "\n"));
	}

	profit = 0;
#if !defined(ADS_ENABLE_FORECAST) && defined(ADS_ENABLE_SANITY_TEST)
	int64_t old_profit = profit;
#define SANITY_TEST_VERIFY_PROFIT \
do {\
	Sanity_Test::instance()->verify_profit_not_decrease(ctx, old_profit, profit); \
	old_profit = profit; \
} while(0)
#else
#define SANITY_TEST_VERIFY_PROFIT \
do {} while(0)
#endif

	for (Ads_Slot_List::iterator it = ctx->parent_slots_.begin(); it != ctx->parent_slots_.end(); ++it)
	{
		Ads_Slot_Base *slot = *it;
		if (slot->env() != ads::ENV_VIDEO)
		{
			continue;
		}
		Ads_Video_Slot* vslot = dynamic_cast<Ads_Video_Slot*>(slot);
		if (vslot == nullptr)
		{
			continue;
		}
		ADS_DEBUG((LP_DEBUG, "    parent slot : %s, max duration %d, max ads %d, max ad duration %d, ad unit restrictions %s\n\n",
			vslot->custom_id_.c_str(),
			vslot->max_duration_,
			vslot->max_num_advertisements_,
			vslot->max_advertisement_duration_,
			ads::entities_to_str(vslot->restriction_ids_).c_str())
		);


		if (vslot->advertisements_.empty())
		{
			vslot->duration_remain_ =  vslot->max_duration_;
			vslot->num_advertisements_ = 0;
		}
	}
	size_t n_slots = slots.size();
	for (size_t i = 0; i < n_slots; ++i)
	{
		Ads_Slot_Base *slot = slots[i];
		ADS_ASSERT(slot->env() == ads::ENV_VIDEO);
		Ads_Video_Slot* vslot = reinterpret_cast<Ads_Video_Slot *>(slot);
		if (slot->advertisements_.empty())
		{
			vslot->duration_remain_ =  vslot->max_duration_;
			vslot->num_advertisements_ = 0;
		}
		if (!slot->has_profile())
		{
			for (Ads_Advertisement_Candidate_List::const_iterator it = candidates.begin(); it != candidates.end(); ++it)
			{
				Ads_Advertisement_Candidate *candidate = *it;
				if (candidate->lite_->is_watched()) ctx->log_watched_info(*candidate->lite_, slot, "NO_PROFILE_FOR_SLOT");
			}
		}
	}
	if (candidates.empty()) return 0;

	auto print_debug_log = [&](int step)
	{
		ADS_DEBUG((LP_TRACE, "Plan after step %d: (%s)\n", step, ads::i64_to_str(profit).c_str()));
		std::map<Ads_Slot_Base *, int> duration_used;
		for (size_t i = 0; i < candidates.size(); ++i)
		{
			if (candidates[i] == nullptr || candidates[i]->slot_ == nullptr || candidates[i]->slot_->env() != ads::ENV_VIDEO)
				continue;
			ADS_DEBUG((LP_TRACE, "    %s:%s\n"
				, CANDIDATE_LITE_LPID_CSTR(candidates[i]->lite_)
				, candidates[i]->slot_ ? candidates[i]->slot_->custom_id_.c_str(): "N/A"));
			duration_used[candidates[i]->slot_] += candidates[i]->creative_duration(candidates[i]->slot_);
		}

		for (const auto &it : duration_used)
		{
			Ads_Slot_Base* slot = it.first;
			int used_duration = it.second;
			int max_duration = reinterpret_cast<Ads_Video_Slot *>(slot)->max_duration_;
			if (max_duration > 0 && used_duration > max_duration)
			{
				// FIXME: break max duration, replace INFO_1__COOKIE_15K
				ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_13);
				ADS_DEBUG((LP_TRACE, "slot %s break max duration restriction, used duration = %d, max duration = %d.\n"
					, slot->custom_id_.c_str(), used_duration, max_duration));
			}
		}
	};

	if (this->knapsack_initialize_solution(ctx, candidates, slots, filters, 0, profit) < 0)
		return -1;
	SANITY_TEST_VERIFY_PROFIT;
	int old_profit = profit;
	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		print_debug_log(1);
	}
	// no knapsack improve for PROPOSAL
	if (ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::FORCE_PROPOSAL)
		return 0;

	// no knapsack improve for specific site section
	if (ctx->section_ && ctx->repository_->system_config().disable_knapsack_improve_sections_.count(ctx->section_->id_) > 0)
	{
		ADS_DEBUG((LP_DEBUG, "Disable knapsack improve for site section %ld\n", ads::entity::id(ctx->root_section_->id_)));
		return 0;
	}

	if (enable_improve(ctx, 1) && this->knapsack_first_improve(ctx, candidates, slots, filters, profit) < 0)
		return -1;
	SANITY_TEST_VERIFY_PROFIT;
	if (profit > old_profit) ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_9); // first improvement
	old_profit = profit;

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		print_debug_log(2);
	}

	if (enable_improve(ctx, 2) && this->knapsack_second_improve(ctx, candidates, slots, filters, profit) < 0)
		return -1;
	SANITY_TEST_VERIFY_PROFIT;
	if (profit > old_profit) ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_12); // second improvement
	old_profit = profit;

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		print_debug_log(3);
	}

	if (enable_improve(ctx, 3) && this->knapsack_third_improve(ctx, candidates, slots, filters, profit) < 0)
		return -1;
	SANITY_TEST_VERIFY_PROFIT;
	if (profit > old_profit) ctx->decision_info_.set_flag1(Decision_Info::INFO_1__FLAG_14); // third improvement

#undef SANITY_TEST_VERIFY_PROFIT
	print_debug_log(4);
	return 0;
}

int
Ads_Selector::second_pass_for_alternative_content_types(Ads_Selection_Context *ctx, Ads_GUID ct, const Ads_Slot_Profile *profile, Ads_Creative_Rendition *creative)
{
	auto repo = ctx->repository_;
	if (!repo) return -1;

	auto range = repo->alternative_content_type_map_.equal_range(ct);
	for (auto it = range.first; it != range.second; ++it)
	{
		auto content_type_id = it->second;
		if (!ads::entity::is_valid_id(content_type_id)) continue;

		if ( repo->compatible(content_type_id, profile->primary_content_types_.begin(), profile->primary_content_types_.end())
			 || repo->implies(content_type_id, profile->primary_content_types_.begin(), profile->primary_content_types_.end(), ctx->network_id_) >= 0)
		{
			auto cit = repo->content_types_.find(content_type_id);
			if (cit == repo->content_types_.end()) continue;
			auto content_type = cit->second->name_.c_str();
			auto mime_type = cit->second->mime_type_.c_str();

			creative->content_type_id_ = content_type_id;
			creative->content_type_ = content_type;
			creative->mime_type_ = mime_type;
			if (creative->primary_asset_)
			{
				creative->primary_asset_->content_type_id_ = content_type_id;
				creative->primary_asset_->content_type_ = content_type;
				creative->primary_asset_->mime_type_ = mime_type;
			}
			if (ctx->verbose_)
				ADS_DEBUG((LP_TRACE,
					   "creative %s rendition %s alternative content type %s is compatible with profile %s\n",
					   ADS_ENTITY_ID_CSTR(creative->creative_id_),
					   CREATIVE_EXTID_CSTR(creative),
					   content_type,
					   profile->name_.c_str()));
			return 0;
		}
	}
	return -1;
}

bool
Ads_Selector::is_creative_wrapper_applicable_for_profile(Ads_Selection_Context *ctx, const Ads_Advertisement *ad, const Ads_Creative_Rendition *creative, const Ads_Slot_Profile *profile, const Ads_Slot_Base *slot, Ads_GUID watched_id)
{
	ADS_ASSERT(creative);
	if (!ads::entity::is_valid_id(creative->wrapper_type_id_))
		return true;

	ADS_ASSERT(ad);
	ADS_ASSERT(profile);
	bool verbose = ctx->verbose_ > 0;
	const Ads_Repository *repo = ctx->repository_;
	bool is_watched = ctx->is_watched(watched_id);

	if (!repo->compatible(creative->wrapper_type_id_, profile->primary_content_types_.begin(), profile->primary_content_types_.end()))
	{
		if (verbose)
			ADS_DEBUG((LP_TRACE,
				"ad %s creative %s rendition %s failed for profile %s type %s(WRAPPER TYPE) \n",
				ADS_ENTITY_ID_CSTR(ad->id_),
				ADS_ENTITY_ID_CSTR(creative->creative_id_),
				CREATIVE_EXTID_CSTR(creative),
				profile->name_.c_str(),
				creative->wrapper_type_.c_str()
			));
		if (is_watched) ctx->log_watched_info(watched_id, slot, creative->id_, profile->name_, "UNSUPPORTED_WRAPPER_TYPE");

		return false;
	}

	if (ctx->request_->rep())
	{
		Ads_Response::FORMAT resp_fmt = Ads_Response::response_format(ctx->request_->rep()->response_format_);
		// to be refined, for now keep previous ad-hoc which let vast-3 wrapper able to be delivered in vast-2 wrapper
		if (resp_fmt == Ads_Response::VAST2)
			resp_fmt = Ads_Response::VAST3;

		int resp_vast_version = Ads_Response::vast_version(resp_fmt);
		if (resp_vast_version > Ads_Response::VAST_V_NONE)
		{
			int creative_vast_version = Ads_Response::VAST_V_NONE;
			if (creative->wrapper_type_ == "external/vast")
				creative_vast_version = Ads_Response::VAST_V1;
			else if (creative->wrapper_type_ == "external/vast-2")
				creative_vast_version = Ads_Response::VAST_V2;
			else if (creative->wrapper_type_ == "external/vast-3")
				creative_vast_version = Ads_Response::VAST_V3;
			else if (creative->wrapper_type_ == "external/vast-4")
				creative_vast_version = Ads_Response::VAST_V4;

			if (creative_vast_version == Ads_Response::VAST_V_NONE ||
				(creative_vast_version == Ads_Response::VAST_V1 && resp_vast_version != Ads_Response::VAST_V1) ||
				creative_vast_version > resp_vast_version)
			{
				if (is_watched) ctx->log_watched_info(watched_id, slot, creative->id_, profile->name_, "VAST_VERSION");
				return false;
			}
		}
	}

	return true;
}
// Only used for linear dai content type check
bool Ads_Selector::is_content_type_compatible(const Ads_Repository &repo, const Ads_Creative_Rendition &rendition, const std::list<Ads_Slot_Profile *> &slot_profiles)
{
	bool content_type_check_pass = false;
	for (std::list<Ads_Slot_Profile *>::const_iterator it = slot_profiles.begin();
		 it != slot_profiles.end();
		 ++it)
	{
		const Ads_Slot_Profile *profile = *it;
		bool primary_content_type_check_pass = true;
		bool secondary_content_type_check_pass = true;
		for (Ads_GUID_RSet::const_iterator it = rendition.primary_content_types_.begin();
			 it != rendition.primary_content_types_.end();
			 ++it)
		{
			if (!repo.compatible(*it, profile->primary_content_types_.begin(), profile->primary_content_types_.end()))
			{
				ADS_DEBUG((LP_DEBUG, "creative %s rendition %s failed for profile %s, content type is %s (SECONDARY_CONTENT_TYPE) \n", ADS_ENTITY_ID_CSTR(rendition.creative_id_), CREATIVE_EXTID_CSTR(&rendition), profile->name_.c_str(), ADS_ENTITY_ID_CSTR(*it)));
				primary_content_type_check_pass = false;
				break;
			}
		}
		if (!primary_content_type_check_pass)
			continue;

		for (Ads_GUID_RSet::const_iterator it = rendition.secondary_content_types_.begin();
			 it != rendition.secondary_content_types_.end();
			 ++it)
		{
			if (!repo.compatible(*it, profile->primary_content_types_.begin(), profile->primary_content_types_.end())
				&& !repo.compatible(*it, profile->content_types_.begin(), profile->content_types_.end()))
			{
				ADS_DEBUG((LP_DEBUG, "creative %s rendition %s failed for profile %s, content type is %s (SECONDARY_CONTENT_TYPE) \n", ADS_ENTITY_ID_CSTR(rendition.creative_id_), CREATIVE_EXTID_CSTR(&rendition), profile->name_.c_str(), ADS_ENTITY_ID_CSTR(*it)));
				secondary_content_type_check_pass = false;
				break;
			}
		}
		if (!secondary_content_type_check_pass)
			continue;
		content_type_check_pass = true;
		break;
	}
	if (!content_type_check_pass)
		return false;
	return true;
}

bool
Ads_Selector::is_creative_bitrate_applicable_for_profile(Ads_Selection_Context *ctx, const Ads_Advertisement *ad, const Ads_Creative_Rendition *creative, const Ads_Slot_Profile *profile, const Ads_Slot_Base *slot, Ads_GUID watched_id, size_t rendition_bitrate)
{
	std::pair<int, int> bitrate_range;
	const Ads_Request::Smart_Rep *smart_rep = ctx->request_->rep();
	bool verbose = ctx->verbose_ > 0;

	bitrate_range.first = (profile->bitrate_range_.first >= 0) ? profile->bitrate_range_.first :
								smart_rep ? smart_rep->key_values_.bitrate_range_.first : -1;
	bitrate_range.second = (profile->bitrate_range_.second >= 0) ? profile->bitrate_range_.second :
								smart_rep ? smart_rep->key_values_.bitrate_range_.second : -1;

	if (bitrate_range.first >= 0)
	{
		if (rendition_bitrate == size_t(-1) || rendition_bitrate < (size_t)bitrate_range.first)
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE, "ad %s creative %s bit rate of rendition %s %d lower than %d for profile %s\n",
						ADS_ENTITY_ID_CSTR(ad->id_),
						ADS_ENTITY_ID_CSTR(creative->creative_id_), CREATIVE_EXTID_CSTR(creative), rendition_bitrate, bitrate_range.first, profile->name_.c_str()));
			if (ctx->is_watched(watched_id)) ctx->log_watched_info(watched_id, slot, creative->id_, profile->name_, "MIN_BITRATE");
			return false;
		}
	}

	if (bitrate_range.second >= 0)
	{
		if (rendition_bitrate == size_t(-1) || rendition_bitrate > (size_t)bitrate_range.second)
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE, "ad %s creative %s bit rate of rendition %s %d exceeds %d for profile %s\n",
						ADS_ENTITY_ID_CSTR(ad->id_),
						ADS_ENTITY_ID_CSTR(creative->creative_id_), CREATIVE_EXTID_CSTR(creative), rendition_bitrate, bitrate_range.second, profile->name_.c_str()));
			if (ctx->is_watched(watched_id)) ctx->log_watched_info(watched_id, slot, creative->id_, profile->name_, "MAX_BITRATE");
			return false;
		}
	}

	return true;
}

bool
Ads_Selector::is_creative_dimension_applicable_for_profile(Ads_Selection_Context *ctx, const Ads_Advertisement &ad, const Ads_Creative_Rendition &rendition, const Ads_Slot_Profile &profile)
{
	if (!ad.is_faked_ad())
		return true;

	auto check = [&](int profile_val, size_t rendition_val, std::function<bool(size_t, size_t)> op)
	{
		if (profile_val == -1)
			return true;
		if (rendition_val == size_t(-1) || !op(profile_val, rendition_val)) {
			ADS_DEBUG((LP_DEBUG, "ad %s creative %s rendition %s fail for profile %s dimension range check (width:%d, height:%d)\n",
						   ADS_ENTITY_ID_CSTR(ad.id_),
						   ADS_ENTITY_ID_CSTR(rendition.creative_id_),
						   ADS_ENTITY_ID_CSTR(rendition.id_),
						   profile.name_.c_str(),
						   rendition.width_, rendition.height_));
			return false;
		}

		return true;
	};

	if (!check(profile.creative_width_range_.first, rendition.width_, std::less_equal<size_t>()))
		return false;

	if (!check(profile.creative_width_range_.second, rendition.width_, std::greater_equal<size_t>()))
		return false;

	if (!check(profile.creative_height_range_.first, rendition.height_, std::less_equal<size_t>()))
		return false;

	if (!check(profile.creative_height_range_.second, rendition.height_, std::greater_equal<size_t>()))
		return false;

	size_t dimension_gcd = std::max(std::__gcd(rendition.width_, rendition.height_), 1ul);
	std::pair<size_t, size_t> dimension_ratio = std::make_pair(rendition.width_ / dimension_gcd, rendition.height_ / dimension_gcd);
	bool is_valid_dimension = rendition.width_ != size_t(-1) && rendition.height_ != size_t(-1);
	if (!profile.dimension_ratio_whitelist_.empty() && (!is_valid_dimension || profile.dimension_ratio_whitelist_.find(dimension_ratio) == profile.dimension_ratio_whitelist_.end()))
	{
		ADS_DEBUG((LP_DEBUG, "ad %s creative %s rendition %s fail for profile %s dimension ratio whitelist check (%d:%d)\n",
						   ADS_ENTITY_ID_CSTR(ad.id_),
						   ADS_ENTITY_ID_CSTR(rendition.creative_id_),
						   ADS_ENTITY_ID_CSTR(rendition.id_),
						   profile.name_.c_str(),
						   dimension_ratio.first, dimension_ratio.second));
		return false;
	}

	if (!profile.dimension_ratio_blacklist_.empty() && (!is_valid_dimension || profile.dimension_ratio_blacklist_.find(dimension_ratio) != profile.dimension_ratio_blacklist_.end()))
	{
		ADS_DEBUG((LP_DEBUG, "ad %s creative %s rendition %s fail for profile %s dimension ratio blacklist check (%d:%d)\n",
						   ADS_ENTITY_ID_CSTR(ad.id_),
						   ADS_ENTITY_ID_CSTR(rendition.creative_id_),
						   ADS_ENTITY_ID_CSTR(rendition.id_),
						   profile.name_.c_str(),
						   dimension_ratio.first, dimension_ratio.second));
		return false;
	}

	return true;
}

bool
Ads_Selector::is_creative_applicable_for_profile(Ads_Selection_Context *ctx, ads::ENVIRONMENT env, const Ads_Advertisement_Candidate *candidate, const Ads_Creative_Rendition *creative, const Ads_Slot_Profile *profile, const Ads_Slot_Base *slot, size_t flags)
{
	selection::Error_Code err = selection::Error_Code::UNDEFINED;
	if (!is_creative_applicable_for_profile(ctx, env, candidate->ad_, creative, profile, slot, flags, &err,
						candidate->lite_->is_watched() ? ctx->get_watched_id(*candidate->lite_) : ads::entity::invalid_id(0)))
	{
		if (err != selection::Error_Code::UNDEFINED)
			ctx->mkpl_executor_->log_ad_error(ctx, *candidate->lite_, selection::Error_Code::UNSUPPORTED_CREATIVE_API, selection::Error_Code::PROFILE_CHECK_FAILED);
		return false;
	}

	return true;
}

bool
Ads_Selector::is_creative_applicable_for_profile(Ads_Selection_Context *ctx, ads::ENVIRONMENT env, const Ads_Advertisement *ad, const Ads_Creative_Rendition *creative, const Ads_Slot_Profile *profile,
						const Ads_Slot_Base *slot, size_t flags, selection::Error_Code *err, Ads_GUID watched_id)
{
#if defined(ADS_ENABLE_FORECAST)
        if ((ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::BYPASS_RESTRICTION)) return true;
#endif
	ADS_ASSERT(profile);
	bool verbose = ctx->verbose_ > 0;
	bool is_watched = ctx->is_watched(watched_id);

	selection::Error_Code dummy_err;
	if (err == nullptr) err = &dummy_err;

	// FDB-8360
	if (profile->flags_ & Ads_Environment_Profile::FLAG_WRAPPED_AD_ONLY && !ads::entity::is_valid_id(creative->wrapper_type_id_))
	{
		if (verbose)
			ADS_DEBUG((LP_TRACE, "ad %s creative %s is NOT applicable for WRAPPED_AD_ONLY profile %s\n",
					   ADS_ENTITY_ID_CSTR(ad->id_),
					   ADS_ENTITY_ID_CSTR(creative->creative_id_), profile->name_.c_str()));
		if (is_watched) ctx->log_watched_info(watched_id, slot, creative->id_, profile->name_, "WRAPPED_AD_ONLY");
		return false;
	}

	// FDB-6937 Renderer Matching by Creative Rendition Actual (Not Display) Size
	if (profile->creative_width_ >= 0 && profile->creative_height_ >= 0)
	{
		if ((creative->width_ != size_t(-1) && creative->width_ != (size_t)profile->creative_width_) || (creative->height_ != size_t(-1) && creative->height_ != (size_t)profile->creative_height_))
		{
			//if (creative->width_ != profile->creative_width_ || creative->height_ != profile->creative_height_)
			{
				if (verbose)
					ADS_DEBUG0((LP_TRACE, "ad %s creative %s wxh of creative %s %dx%d not match %dx%d for profile %s\n",
						ADS_ENTITY_ID_CSTR(ad->id_),
						ADS_ENTITY_ID_CSTR(creative->creative_id_),
						CREATIVE_EXTID_CSTR(creative),
						creative->width_, creative->height_,
						profile->creative_width_, profile->creative_height_,
						profile->name_.c_str()));
				return false;
			}
		}
	}

	if (env == ads::ENV_VIDEO)
	{
		if (!is_creative_dimension_applicable_for_profile(ctx, *ad, *creative, *profile))
			return false;

		/// frame rate
		if (profile->frame_rate_range_.first >= 0)
		{
			if (creative->frame_rate_ < 0 || creative->frame_rate_ < profile->frame_rate_range_.first)
			{
				if (verbose)
					ADS_DEBUG((LP_TRACE, "ad %s creative %s frame rate of rendition %s %f lower than %f for profile %s\n",
						ADS_ENTITY_ID_CSTR(ad->id_), ADS_ENTITY_ID_CSTR(creative->creative_id_), CREATIVE_EXTID_CSTR(creative),
						creative->frame_rate_, profile->frame_rate_range_.first, profile->name_.c_str()));
				if (is_watched) ctx->log_watched_info(watched_id, slot, creative->id_, profile->name_, "MIN_FRAMERATE");
				return false;
			}
		}

		if (profile->frame_rate_range_.second >= 0)
		{
			if (creative->frame_rate_ < 0 || creative->frame_rate_ > profile->frame_rate_range_.second)
			{
				if (verbose)
					ADS_DEBUG((LP_TRACE, "ad %s creative %s frame rate of rendition %s %f exceeds %f for profile %s\n",
						ADS_ENTITY_ID_CSTR(ad->id_), ADS_ENTITY_ID_CSTR(creative->creative_id_),
						CREATIVE_EXTID_CSTR(creative), creative->frame_rate_, profile->frame_rate_range_.second, profile->name_.c_str()));
				if (is_watched) ctx->log_watched_info(watched_id, slot, creative->id_, profile->name_, "MAX_FRAMERATE");
				return false;
			}
		}

		/// bit rate
		size_t rendition_bitrate = size_t(-1);
		// PUB-4417: do not use ad asset store bitrate for VOD vast creative rendation,
		// since MRM does not have vast creative rendation info in ad asset store.
		// Note: ad_asset_stores_('_fw_ass') is only used in VOD integration.
		if (!ctx->ad_asset_stores_.empty() && !creative->is_vast_rendition())
		{
			auto store_it = creative->ad_asset_store_info_.find(ctx->ad_asset_stores_[0]);
			if (store_it != creative->ad_asset_store_info_.end())
				rendition_bitrate = store_it->second.bitrate_;
		}
		else
		{
			rendition_bitrate = creative->bitrate_;
		}
		if (!is_creative_bitrate_applicable_for_profile(ctx, ad, creative, profile, slot, watched_id, rendition_bitrate))
		{
			return false;
		}

		/// aspect ratio
		if (slot && (profile->scale_range_.first >= 0 || profile->scale_range_.second >= 0)
			&& ((slot->height_ > 0 && slot->width_ > 0) ||slot->compatible_dimensions_.size() > 1))
		{
			if (creative->width_ == 0 || creative->height_ == 0 || creative->width_ == size_t(-1) || creative->height_ == size_t(-1))
			{
				if (verbose)
					ADS_DEBUG((LP_TRACE, "ad %s creative %s dimension of rendition %s has nontrivial wxh:%dx%d for profile %s\n",
						ADS_ENTITY_ID_CSTR(ad->id_), ADS_ENTITY_ID_CSTR(creative->creative_id_),
						CREATIVE_EXTID_CSTR(creative), creative->width_, creative->height_, profile->name_.c_str()));
				if (is_watched) ctx->log_watched_info(watched_id, slot, creative->id_, profile->name_, "NONTRIVIAL_WIDTH_HEIGHT");
				return false;
			}

			bool frame_size_applicable = false;
			for (const auto& d : slot->compatible_dimensions_)
			{
				size_t slot_width = d.first;
				size_t slot_height = d.second;
				double scale = 0;
				if (creative->width_ * slot_height > creative->height_ * slot_width) /* equals to (creative->width_ / creative->height_) > (slot_width / slot_height) */
				{
					///creative is wider than slot
					scale = (double)slot_width / creative->width_;
				}
				else
				{
					///creative is higher than slot
					scale = (double)slot_height / creative->height_;
				}

				if (profile->scale_range_.first >= 0 && scale < (1 - profile->scale_range_.first))
				{
					if (verbose)
						ADS_DEBUG((LP_TRACE, "ad %s creative %s dimension of rendition %s wxh:%dx%d on slot %s wxh:%dx%d lower than %f for profile %s\n",
								ADS_ENTITY_ID_CSTR(ad->id_),
								ADS_ENTITY_ID_CSTR(creative->creative_id_),
								CREATIVE_EXTID_CSTR(creative), creative->width_, creative->height_,
								slot->custom_id_.c_str(), slot_width, slot_height,
								profile->scale_range_.first, profile->name_.c_str()));
					if (is_watched) ctx->log_watched_info(watched_id, slot, creative->id_, profile->name_, "MIN_SCALEDOWN");
					continue;
				}

				if (profile->scale_range_.second >= 0 && scale > (1 + profile->scale_range_.second))
				{
					if (verbose)
						ADS_DEBUG((LP_TRACE, "ad %s creative %s dimension of rendition %s wxh:%dx%d on slot %s wxh:%dx%d exceeds %f for profile %s\n",
								ADS_ENTITY_ID_CSTR(ad->id_),
								ADS_ENTITY_ID_CSTR(creative->creative_id_),
								CREATIVE_EXTID_CSTR(creative), creative->width_, creative->height_,
								slot->custom_id_.c_str(), slot_width, slot_height,
								profile->scale_range_.second, profile->name_.c_str()));
					if (is_watched) ctx->log_watched_info(watched_id, slot, creative->id_, profile->name_, "MAX_SCALEUP");
					continue;
				}
				frame_size_applicable = true;
			}

			if (!frame_size_applicable) return false;
		}
	}

	if (!profile->ad_units_.empty() && profile->ad_units_.find(ad->ad_unit()->effective_id()) == profile->ad_units_.end())
	{
		if (verbose)
			ADS_DEBUG((LP_TRACE,
			           "ad %s unit %s creative %s rendition %s failed for profile %s (AD_UNIT)\n",
			           ADS_ENTITY_ID_CSTR(ad->id_),
			           ADS_ENTITY_ID_CSTR(ad->ad_unit() ? ad->ad_unit()->id_ : 0),
			           ADS_ENTITY_ID_CSTR(creative->creative_id_),
			           CREATIVE_EXTID_CSTR(creative),
			           profile->name_.c_str())
			         );
		if (is_watched) ctx->log_watched_info(watched_id, slot, creative->id_, profile->name_, "UNMATCHED_AD_UNIT");
		return false;
	}

	if (profile->base_ad_units_.find(creative->base_ad_unit_) == profile->base_ad_units_.end())
	{
		if (verbose)
			ADS_DEBUG((LP_TRACE,
			           "ad %s creative %s rendition %s failed for profile %s (BASE_AD_UNIT)\n",
			           ADS_ENTITY_ID_CSTR(ad->id_),
			           ADS_ENTITY_ID_CSTR(creative->creative_id_),
			           CREATIVE_EXTID_CSTR(creative),
			           profile->name_.c_str())
			         );
		if (is_watched) ctx->log_watched_info(watched_id, slot, creative->id_, profile->name_, "UNSUPPORTED_BASE_AD_UNIT");
		return false;
	}

	const Ads_Repository *repo = ctx->repository_;

	if (flags)
	{
		if (!is_creative_wrapper_applicable_for_profile(ctx, ad, creative, profile, slot, watched_id))
			return false;
	}
	else
	{
		Ads_GUID creative_api_id = creative->creative_api_id_;
		if (!ads::entity::is_valid_id(creative_api_id)) creative_api_id = ads::entity::make_id(ADS_ENTITY_TYPE_CREATIVE_API, 100);

		if (!profile->creative_apis_.empty()
		        && profile->creative_apis_.find(creative_api_id) == profile->creative_apis_.end())
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE,
				           "ad %s creative %s rendition %s failed for profile %s, creative api %s (CREATIVE_API)\n",
				           ADS_ENTITY_ID_CSTR(ad->id_),
				           ADS_ENTITY_ID_CSTR(creative->creative_id_),
				           CREATIVE_EXTID_CSTR(creative),
				           profile->name_.c_str(),
				           ADS_ENTITY_ID_CSTR(creative_api_id))
				         );
			if (is_watched) ctx->log_watched_info(watched_id, slot, creative->id_, profile->name_, "UNSUPPORTED_CREATIVE_API");

			*err = selection::Error_Code::UNSUPPORTED_CREATIVE_API;
			return false;
		}

		for (Ads_GUID_RSet::const_iterator it = creative->primary_content_types_.begin();
		        it != creative->primary_content_types_.end();
		        ++it)
		{
			if (!repo->compatible(*it, profile->primary_content_types_.begin(), profile->primary_content_types_.end())
			        && repo->implies(*it, profile->primary_content_types_.begin(), profile->primary_content_types_.end(), ctx->network_id_) < 0)
			{
				if (verbose)
					ADS_DEBUG((LP_TRACE,
					           "ad %s creative %s rendition %s failed for profile %s, content type %s (PRIMARY CONTENT TYPE) \n",
					           ADS_ENTITY_ID_CSTR(ad->id_),
					           ADS_ENTITY_ID_CSTR(creative->creative_id_),
					           CREATIVE_EXTID_CSTR(creative),
					           profile->name_.c_str(),
					           ADS_ENTITY_ID_CSTR(*it)));

				// try alternative content types for external creative
				if (creative->rendition_ext_info_ && second_pass_for_alternative_content_types(ctx, *it, profile, const_cast<Ads_Creative_Rendition*>(creative)) >= 0)
					continue;
				if (is_watched) ctx->log_watched_info(watched_id, slot, creative->id_, profile->name_, "UNSUPPORTED_PRIMARY_CONTENT_TYPE");
				return false;
			}
		}

		if (!ctx->bypass_secondary_rendition_asset_content_type_check_)
		{
			for (Ads_GUID secondary_content_type_id : creative->secondary_content_types_)
			{
				if (!repo->compatible(secondary_content_type_id, profile->primary_content_types_.begin(), profile->primary_content_types_.end())
				    && !repo->compatible(secondary_content_type_id, profile->content_types_.begin(), profile->content_types_.end()))
				{
					if (verbose)
						ADS_DEBUG((LP_TRACE,
							"ad %s creative %s rendition %s failed for profile %s, content type %s (SECONDARY_CONTENT_TYPE) \n",
							ADS_ENTITY_ID_CSTR(ad->id_),
							ADS_ENTITY_ID_CSTR(creative->creative_id_),
							CREATIVE_EXTID_CSTR(creative),
							profile->name_.c_str(),
							ADS_ENTITY_ID_CSTR(secondary_content_type_id)));
					if (is_watched) ctx->log_watched_info(watched_id, slot, creative->id_, profile->name_, "UNSUPPORTED_SECONDARY_CONTENT_TYPE");
					return false;
				}
			}
		}
		else
		{
			ADS_DEBUG((LP_DEBUG, "bypass secondary content type check according to the profile setting. profile %s\n", profile->name_.c_str()));
		}
	}

	return true;
}

bool
Ads_Selector::is_creative_applicable_for_profiles(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate *candidate, const Ads_Creative_Rendition *creative,
							const Ads_Slot_Base *slot, selection::Error_Code &err, const Profile_Unmatch_Handler &profile_unmatch_handler)
{
	const Ads_Slot_Profile *applicable_profile = nullptr;
	const Ads_Advertisement_Candidate_Lite *lite = candidate->lite_;

	auto applicable_for_jitt = [&]()
	{
		const auto* ad_unit = lite->ad_unit();
		if (ad_unit == nullptr || ad_unit->slot_type_ != Ads_Ad_Unit::VIDEO)
			return false;

		if (lite->is_pre_selection_external_translated())
		{
			const auto *external_creative_info = lite->external_creative_info(creative->creative_);
			return external_creative_info != nullptr && !external_creative_info->cch_key_.empty();
		}
		else if (ads::entity::is_valid_id(creative->wrapper_type_id_))
			return false;
		else
		{
			return creative->creative_ != nullptr && creative->creative_->is_transcodable_nonwrapper_creative_;
		}
	};

	do {
		Ads_Advertisement_Candidate_Lite::JiTT_Config::OCCASION jit_occasion = lite->is_pre_selection_external_translated() ?
											Ads_Advertisement_Candidate_Lite::JiTT_Config::MARKET :
											(ads::entity::is_valid_id(creative->wrapper_type_id_) ?
											Ads_Advertisement_Candidate_Lite::JiTT_Config::VAST_NON_MARKET :
											Ads_Advertisement_Candidate_Lite::JiTT_Config::NON_VAST);
		if (lite->jitt_enablement(jit_occasion).is_mandatory() && applicable_for_jitt()
				&& creative->transcode_trait_ == nullptr)
		{
			ADS_DEBUG((LP_DEBUG,
					"ad %s creative %s rendition %s failed for profiles (JITT RENDITION REQUIRED)\n",
					ADS_ENTITY_ID_CSTR(candidate->ad_id()),
					ADS_ENTITY_ID_CSTR(creative->creative_id_),
					CREATIVE_EXTID_CSTR(creative))
					);
			break;
		}

		if (lite->operation_owner().network_.config()->enable_market_creative_replacement_ && lite->external_creative_info(creative->creative_) && lite->external_creative_info(creative->creative_)->client_renditions_.empty() && creative->transcode_trait_ ==  nullptr)
		{
			if (lite->is_watched())
				ctx->log_watched_info(*lite, slot, creative->creative_id_, "CLIENT_RENDITION_REQUIRED");
			ADS_DEBUG((LP_DEBUG,
					"ad %s creative %s rendition %s failed for profiles (CLIENT RENDITION REQUIRED)\n",
					ADS_ENTITY_ID_CSTR(candidate->ad_id()),
					ADS_ENTITY_ID_CSTR(creative->creative_id_),
					CREATIVE_EXTID_CSTR(creative))
					);
			break;
		}

		for (const Ads_Slot_Profile *profile : slot->profiles_)
		{
			if (is_creative_applicable_for_profile(ctx, slot->env(), candidate, creative, profile, slot, 0))
			{
				applicable_profile = profile;
				break;
			}
		}
	} while(0);

	// OPP-6389 bypass profile matching logic
	bool bypassed = false;
	if (applicable_profile == nullptr && profile_unmatch_handler != nullptr)
	{
		profile_unmatch_handler(creative, slot, bypassed);
	}

	bool applicable = (applicable_profile != nullptr || bypassed);
	/// second pass for wrapper type
	if (applicable && ads::entity::is_valid_id(creative->wrapper_type_id_))
	{
		bool found = false;
		for (const Ads_Slot_Profile *profile : slot->profiles_)
		{
			bool is_wrapper_applicable = bypassed ? is_creative_wrapper_applicable_for_profile(ctx, candidate->ad_, creative, profile, slot, lite->is_watched() ? ctx->get_watched_id(*lite) : ads::entity::invalid_id(0))
								: is_creative_applicable_for_profile(ctx, slot->env(), candidate, creative, profile, slot, 1);
			if (is_wrapper_applicable)
			{
				found = true;
				break;
			}
		}

		if (!found)
		{
			applicable = false;
		}
	}

	if (applicable)
	{
		if (applicable_profile != nullptr)
		{
			ADS_DEBUG((LP_DEBUG, "SUCCESS: ad %s creative %s rendition %s, an applicable profile %s found for slot %s[%d]%s\n",
					CANDIDATE_LITE_ID_CSTR(lite), CREATIVE_EXTID_CSTR(creative->creative_), CREATIVE_EXTID_CSTR(creative),
					applicable_profile->name_.c_str(), ads::env_name(slot->env()), slot->position_, slot->custom_id_.c_str()));
			if (lite->is_watched()) ctx->log_watched_info(*lite, slot, creative->id_, "PROFILE_FOUND");
		}
	}
	else
	{
		ADS_DEBUG((LP_DEBUG, "ad %s creative %s rendition %s, no applicable profile found for slot %s\n", CANDIDATE_LITE_ID_CSTR(lite), ADS_ENTITY_ID_CSTR(creative->creative_id_), CREATIVE_EXTID_CSTR(creative), slot->custom_id_.c_str()));
		err = selection::Error_Code::NO_APPLICABLE_PROFILES_FOR_RENDITION;
		ctx->mkpl_executor_->log_ad_error(ctx, *lite, err, selection::Error_Code::PROFILE_CHECK_FAILED);
	}
	return applicable;
}

bool
Ads_Selector::is_creative_applicable_for_request(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate_Lite *candidate, const Ads_Creative_Rendition *rendition, const Ads_Advertisement::Rendition_Info &rendition_info)
{
#if defined(ADS_ENABLE_FORECAST)
	if(ads::entity::is_valid_id(rendition->placeholder_opportunity_id_) || ads::entity::is_valid_id(rendition->placeholder_sequence_)) /*missing creative with placeholder for hylda*/
		return false;
	if ((ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::BYPASS_RESTRICTION)) return true;
#endif

	bool verbose = ctx->verbose_ > 0;
	const Ads_Advertisement *ad = candidate->ad_;
	const std::pair<time_t, time_t>& date_range = std::make_pair(rendition_info.start_date, rendition_info.end_date);

	// flight date
	if (!ad->is_tracking() && !ctx->in_date_range(date_range.first, date_range.second))
	{
		if (verbose)
			ADS_DEBUG((LP_TRACE,
				"delivery date test failed for ad creative %s, %s: %d,%d\n", CANDIDATE_LITE_ID_CSTR(candidate), ADS_ENTITY_ID_CSTR(rendition->id_), date_range.first, date_range.second));
		if (candidate->is_watched()) ctx->log_watched_info(*candidate, nullptr, rendition->id_, "DATE_RANGE");
		return false;
	}

	// check targeting tree for creative/rendition
	Ads_Asset_Section_Closure_Map::iterator cit = ctx->closures_.find(candidate->network_id());
	if (rendition_info.criteria != nullptr && cit != ctx->closures_.end() && cit->second && cit->second->is_active())
	{
		Ads_Asset_Section_Closure * closure = cit->second;

		if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
		{
			ADS_DEBUG0((LP_TRACE,
					"creative targeting check: extract terms from closure %s (total %d): ", ADS_LOOPED_ID_CSTR(closure->network_id_), closure->terms_.size()));
			for (Ads_GUID_Set::iterator it = closure->terms_.begin(); it != closure->terms_.end(); ++it)
				ADS_DEBUG0((LP_TRACE, "%s, ", ads::term::str(*it).c_str()));
			ADS_DEBUG0((LP_TRACE, "\n"));
		}

		bool all_terms_ignored = false;
		Ads_GUID_List path;
		bool res = test_partial_targeting_criteria(ctx->repository_, ctx->delta_repository_, closure->terms_, rendition_info.criteria, nullptr, all_terms_ignored, path, nullptr);
		if(res == false)
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE, "creative targeting check: ad id %s, rendition id %s banned.\n", CANDIDATE_LITE_ID_CSTR(candidate), ADS_ENTITY_ID_CSTR(rendition->id_)));
			if (candidate->is_watched()) ctx->log_watched_info(*candidate, nullptr, rendition->id_, "CREATIVE_LEVEL_TARGETING_NOT_MATCH");
			return false;
		}
	}

	/// flash compatibility check
	if (!ctx->request_->flash_version().empty() && !candidate->is_two_phase_translated_ad() && !candidate->is_pg_td_ad())
	{
		if (!this->check_creative_rendition_flash_compatibility(ctx, ctx->request_->flash_version(), rendition))
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s incompatible flash version %s\n",
				           CANDIDATE_LITE_ID_CSTR(candidate),
				           ADS_ENTITY_ID_CSTR(rendition->creative_id_),
				           ADS_ENTITY_ID_CSTR(rendition->id_), ctx->request_->flash_version().c_str()));
			if (candidate->is_watched()) ctx->log_watched_info(*candidate, nullptr, rendition->id_, "INCOMPATIBLE_FLASH_VERSION");

			ctx->mkpl_executor_->log_ad_error(ctx, *candidate, selection::Error_Code::INCOMPATIBLE_FLASH_VERSION, selection::Error_Code::PROFILE_CHECK_FAILED);
			return false;
		}
	}


	bool check_rating = !ad->is_external_
#if !defined(ADS_ENABLE_FORECAST)
		/// TODO: fix MRM-7295
		&& ad->rating_type_ == Ads_Ratable::RATING_TYPE_NONE
#endif
	;

	// rating
	if (check_rating && !Ads_Ratable::match(ctx->restriction_.rating_.first, ctx->restriction_.rating_.second, rendition->rating_type_, rendition->rating_value_))
	{
		ctx->mkpl_executor_->log_ad_error(ctx, *candidate, selection::Error_Code::RATING_RESTRICTION);
		ADS_DEBUG((LP_TRACE,
		           "rating test failed for ad creative %s, %s(%d,%d), asset (%d,%d)\n", ADS_ADVERTISEMENT_ID_CSTR(ad->id_), ADS_ENTITY_ID_CSTR(rendition->id_), rendition->rating_type_, rendition->rating_value_, ctx->restriction_.rating_.first, ctx->restriction_.rating_.second));
		if (candidate->is_watched()) ctx->log_watched_info(*candidate, nullptr, rendition->id_, "RATING " + ctx->restriction_.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_RATING, -1));

		return false;
	}

	//FDB-8442 SSL support
	if (ctx->request_->is_secure())
	{
		if (!(rendition->compatibility_ & Ads_Creative_Rendition::HTTPS))
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s failed (creative could not served in HTTPS)\n"
						   , CANDIDATE_LITE_ID_CSTR(candidate), ADS_ENTITY_ID_CSTR(rendition->creative_id_), ADS_ENTITY_ID_CSTR(rendition->id_)));
			if (candidate->is_watched()) ctx->log_watched_info(*candidate, nullptr, rendition->id_, "AD_ASSET_HTTPS_AVAILABILITY");
			return false;
		}
	}
	else
	{
		if (!(rendition->compatibility_ & Ads_Creative_Rendition::HTTP))
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s failed (creative could not served in HTTP)\n"
						   , CANDIDATE_LITE_ID_CSTR(candidate), ADS_ENTITY_ID_CSTR(rendition->creative_id_), ADS_ENTITY_ID_CSTR(rendition->id_)));
			if (candidate->is_watched()) ctx->log_watched_info(*candidate, nullptr, rendition->id_, "AD_ASSET_HTTP_AVAILABILITY");
			return false;
		}
	}

	//FDB 6781 Ad Asset Availability
	//FDB 14696 rework
	//OPP-1651: skip ad asset availability check for VAST creative rendition.
	const auto* ad_unit = candidate->ad_unit();
	if (ad_unit != nullptr && ad_unit->slot_type_ == Ads_Ad_Unit::VIDEO &&
		!rendition->is_vast_rendition() &&
		!candidate->is_pg_td_ad() && !candidate->is_two_phase_translated_ad())
	{
		bool applicable = false;
		bool is_config_error = false;
		do
		{
			if (ctx->request_ && ctx->request_->is_scte_130_resp() && candidate->is_pre_selection_external_translated() &&
					rendition->primary_asset_ != nullptr &&
					!Ads_Selector::is_vod_primary_asset_location_valid(rendition->primary_asset_->location_.c_str()))
			{
				ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s failed (vod location check)\n"
							, CANDIDATE_LITE_ID_CSTR(candidate), ADS_ENTITY_ID_CSTR(rendition->creative_id_), ADS_ENTITY_ID_CSTR(rendition->id_)));
				break;
			}

			if (!is_creative_ad_asset_store_applicable_for_request(ctx, rendition, &rendition->ad_asset_store_info_))
			{
				ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s failed (ad asset store available check)\n"
							, CANDIDATE_LITE_ID_CSTR(candidate), ADS_ENTITY_ID_CSTR(rendition->creative_id_), ADS_ENTITY_ID_CSTR(rendition->id_)));
				if (candidate->is_watched()) ctx->log_watched_info(*candidate, nullptr, rendition->id_, "AD_ASSET_STORE_AVAILABILITY");
				break;
			}

//ESC-26830 fix, skip faked_ creative bit rate check
#if defined(ADS_ENABLE_FORECAST)
			if (!rendition->faked_)
#endif
			{
				if (!is_creative_bitrate_applicable_for_request(ctx, rendition, &rendition->ad_asset_store_info_, &is_config_error))
				{
					ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s failed (ad asset store bitrate check)\n"
								, CANDIDATE_LITE_ID_CSTR(candidate), ADS_ENTITY_ID_CSTR(rendition->creative_id_), ADS_ENTITY_ID_CSTR(rendition->id_)));
					if (candidate->is_watched()) ctx->log_watched_info(*candidate, nullptr, rendition->id_, "CREATIVE_BITRATE_UNMATCH");
					break;
				}
			}
			applicable = true;
		} while (false);

		if (!applicable)
		{
			if (rendition->creative_ != nullptr && !is_config_error)
				candidate->jit_candidate_creatives_[nullptr].emplace(rendition->creative_);
			return false;
		}
	}

	return true;
}

bool
Ads_Selector::is_creative_ad_asset_store_applicable_for_request(const Ads_Selection_Context *ctx, const Ads_Creative_Rendition *creative, const Ads_Ad_Asset_Store_Info_Map *ad_asset_store_info)
{
	if (!ctx->ad_asset_stores_.empty())
	{
		bool found = false;
		for (const auto& asset_store : ctx->ad_asset_stores_)
		{
			time_t now = ::time(NULL);
#if defined(ADS_ENABLE_FORECAST)
			now = ads::str_to_i64(ctx->request_->overrides_.request_time_);
#endif
			if (ad_asset_store_info == nullptr)
				return false;
			auto sit = ad_asset_store_info->find(asset_store);
#if defined(ADS_ENABLE_FORECAST)
			if (ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::BYPASS_RESTRICTION)
			{
				if (sit != ad_asset_store_info->end())
				{
					found = true;
				}
			}
			else
			{
				if (sit != ad_asset_store_info->end() && (sit->second.expired_at_ > now))
				{
					found = true;
				}
			}
#else
			if (sit != ad_asset_store_info->end() && (sit->second.expired_at_ > now))
			{
				found = true;
			}
#endif
			if (found) break;
		}
#if defined(ADS_ENABLE_FORECAST)
		if (creative->faked_) found = true; //dummy creative should pass asset store check
#endif
		return found;
	}

	return true;
}

bool
Ads_Selector::is_creative_bitrate_applicable_for_request(const Ads_Selection_Context *ctx, const Ads_Creative_Rendition *creative, const Ads_Ad_Asset_Store_Info_Map *ad_asset_store_info, bool *is_config_error)
{
	bool dummy_is_config_error;
	if (is_config_error == nullptr)
		is_config_error = &dummy_is_config_error;
	*is_config_error = false;

	if (ctx->request_->rep() && ctx->check_bitrate_)
	{
		int asset_bitrate = ctx->request_->rep()->key_values_.bitrate_;
		if (asset_bitrate > 0)
		{
			if (ctx->ad_asset_stores_.empty())
			{
				*is_config_error = true;
				ADS_DEBUG((LP_DEBUG, "check_bitrate is true while no valid asset store in request.\n"));
				return false;
			}
			if (ad_asset_store_info == nullptr)
				return false;
			auto sit = ad_asset_store_info->find(ctx->ad_asset_stores_[0]);
			if (sit == ad_asset_store_info->end())
			{
				ADS_DEBUG((LP_DEBUG, "creative %s rendition %s can not find the asset store from request(id:%d).\n",
					ADS_ENTITY_ID_CSTR(creative->creative_id_), ADS_ENTITY_ID_CSTR(creative->id_), ctx->ad_asset_stores_[0]));
				return false;
			}
			if (sit->second.bitrate_  > (size_t)asset_bitrate)
			{
				ADS_DEBUG((LP_TRACE, "creative %s rendition %s bitrate %d mismatch asset bitrate %d\n",
					ADS_ENTITY_ID_CSTR(creative->creative_id_), ADS_ENTITY_ID_CSTR(creative->id_), sit->second.bitrate_, asset_bitrate));
				return false;
			}
		}
		else
		{
			*is_config_error = true;
			ADS_DEBUG((LP_TRACE, "entertainment asset bitrate is not set\n"));
			return false;
		}
	}

	return true;
}

bool
Ads_Selector::is_creative_applicable_for_request_restrictions(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate *candidate, const Ads_Creative_Rendition *creative, selection::Error_Code& error_code)
{
	if (ctx == NULL || candidate == NULL)
	{
		ADS_DEBUG((LP_ERROR, "ctx or candidate is NULL: %p/%p\n", ctx, candidate));
		return false;
	}

	const Ads_Advertisement_Candidate_Lite *lite = candidate->lite_;
	bool verbose = ctx->verbose_ > 0;

	///ad creative level category blacklist
	if (creative && !creative->ad_categories().empty())
	{
		if (!ctx->restriction_.ad_category_blacklist_.empty())
		{
			const Ads_GUID_Set & blacklist = ctx->restriction_.ad_category_blacklist_;
			for (Ads_GUID_RSet::const_iterator ait=creative->ad_categories().begin();  ait != creative->ad_categories().end(); ++ait)
			{
				if (blacklist.find(*ait) != blacklist.end())
				{
					if (verbose)
						ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s failed (creative ad category blacklist)\n"
						, CANDIDATE_LITE_ID_CSTR(lite), ADS_ENTITY_ID_CSTR(creative->creative_id_), ADS_ENTITY_ID_CSTR(creative->id_)));
					if (candidate->lite_->is_watched()) ctx->log_watched_info(*candidate->lite_, nullptr, creative->id_, "AD_CATEGORY_BLACKLIST " + ctx->restriction_.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_CATEGORY, *ait));
					return false;
				}
			}
		}

		if (candidate->restriction(candidate->assoc_) &&
			!candidate->restriction(candidate->assoc_)->ad_category_blacklist_.empty())
		{
			const Ads_GUID_Set &blacklist = candidate->restriction(candidate->assoc_)->ad_category_blacklist_;
			for (Ads_GUID_RSet::const_iterator ait=creative->ad_categories().begin();  ait != creative->ad_categories().end(); ++ait)
			{
				if (blacklist.find(*ait) != blacklist.end())
				{
					if (verbose)
						ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s failed (creative ad category blacklist)\n"
						, CANDIDATE_LITE_ID_CSTR(lite), ADS_ENTITY_ID_CSTR(creative->creative_id_), ADS_ENTITY_ID_CSTR(creative->id_)));
					if (candidate->lite_->is_watched()) ctx->log_watched_info(*candidate->lite_, nullptr, creative->id_, "AD_CATEGORY_BLACKLIST " + candidate->restriction(candidate->assoc_)->info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_CATEGORY, *ait));
					return false;
				}
			}
		}
	}
	if (!this->is_creative_applicable_for_clearcast_restrictions(ctx, candidate, creative, ctx->restriction_.clearcast_code_blacklist_))
	{
		error_code = selection::Error_Code::CLEARCAST_CODE_RESTRICTED;
		return false;
	}
	if (candidate->restriction(candidate->assoc_) != nullptr && !this->is_creative_applicable_for_clearcast_restrictions(ctx, candidate, creative, candidate->restriction(candidate->assoc_)->clearcast_code_blacklist_))
	{
		error_code = selection::Error_Code::CLEARCAST_CODE_RESTRICTED;
		return false;
	}
	return true;
}

bool
Ads_Selector::is_creative_applicable_for_triggering_concrete_event(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate *candidate, const Ads_Creative_Rendition *creative)
{
	if (candidate->ad_->budget_exempt_)
		return true;

	auto repo = ctx->repository_;
	auto cit = repo->event_supported_content_types_.find(candidate->triggering_concrete_event_id_);
	if (cit == repo->event_supported_content_types_.end())
		return true;

	bool verbose = ctx->verbose_ > 0;
	bool applicable = false;
	Ads_GUID_RSet content_types = cit->second;
	if (content_types.empty())
		return true;

#if defined(ADS_ENABLE_FORECAST)
	if (creative->primary_content_types_.empty() && creative->faked_)
		applicable = true;
#endif
	for (auto src_content_type_id : creative->primary_content_types_)
	{
		//only check exactly match
		if (repo->compatible(src_content_type_id, content_types.begin(), content_types.end()))
		{
			applicable = true;
			break;
		}
	}

	if (!applicable)
	{
		if (verbose)
			ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s failed for triggering concrete event %s (PRIMARY CONTENT TYPE) \n",
						CANDIDATE_LITE_ID_CSTR(candidate->lite_),
						ADS_ENTITY_ID_CSTR(creative->creative_id_),
						ADS_ENTITY_ID_CSTR(creative->id_),
						ADS_ENTITY_ID_CSTR(candidate->triggering_concrete_event_id_)));
		if (candidate->lite_->is_watched()) ctx->log_watched_info(*candidate->lite_, nullptr, creative->id_, "TRIGGERING_EVENT_REQUIRED_TYPE_NOT_PRESENT");
	}

	return applicable;
}

bool
Ads_Selector::is_creative_applicable_for_rbp_proxy_content_type(Ads_Selection_Context& ctx, const Ads_Advertisement_Candidate& candidate, const Ads_Creative_Rendition& creative)
{
	if(!candidate.ad_ || !candidate.ad_->placement_ || !ctx.repository_ || !candidate.lite_)
		return true;

	if(candidate.ad_->budget_exempt_)
		return true;

	if(candidate.ad_->placement_->rbp_proxy_content_types_.empty())
		return true;

	auto repo = ctx.repository_;
	bool applicable = false;
	bool verbose = ctx.verbose_ > 0;
	const Ads_GUID_RSet& content_types = candidate.ad_->placement_->rbp_proxy_content_types_;
	for(const auto& src_content_type_id : creative.primary_content_types_)
	{
		if(repo->compatible(src_content_type_id, content_types.begin(), content_types.end()))
		{
			applicable = true;
			break;
		}
	}

	if(!applicable)
	{
		if(verbose)
		{
			ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s failed for rbp content type check\n",
					   CANDIDATE_LITE_ID_CSTR(candidate.lite_), ADS_ENTITY_ID_CSTR(creative.creative_id_), ADS_ENTITY_ID_CSTR(creative.id_)));
		}
		if(candidate.lite_->is_watched())
			ctx.log_watched_info(*(candidate.lite_), nullptr, creative.id_, "RBP_PROXY_RENDITION_REQUIRED_CONTENT_TYPE_NOT_PRESENT");
	}
	return applicable;
}

bool
Ads_Selector::is_creative_applicable_for_clearcast_restrictions(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate *candidate, const Ads_Creative_Rendition *creative, const Ads_GUID_Set& blacklist)
{
	if (creative == nullptr)
		return true;
	const auto& clearcast_codes = creative->clearcast_info().code_ids();

	bool is_source_external = creative->clearcast_info().compliance_source_ == Ads_Compliance_Source::CCH;
	bool applicable = true, checked = false, verbose = ctx->verbose_ > 0;
	bool is_clearcast_codes_empty = clearcast_codes.empty() ||
					std::all_of(clearcast_codes.begin(), clearcast_codes.end(), Ads_Creative_Rendition::Clearcast_Info::is_internal_code_id);
	if (!blacklist.empty())
	{
		checked = true;
		for (const auto& code : clearcast_codes)
		{
			if (blacklist.find(code) != blacklist.end())
			{
				if (verbose)
					ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s failed (creative clearcast code blacklist)\n"
					, CANDIDATE_LITE_ID_CSTR(candidate->lite_), ADS_ENTITY_ID_CSTR(creative->creative_id_), ADS_ENTITY_ID_CSTR(creative->id_)));
				if (candidate->lite_->is_watched()) ctx->log_watched_info(*candidate->lite_, nullptr, creative->id_, "CLEARCAST_CODE_BLACKLIST " + ctx->restriction_.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_CLEARCAST_CODE, code));
				applicable = false;
				break;
			}
		}
		if (applicable && is_source_external && is_clearcast_codes_empty)
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s failed (empty external creative clearcast code but clearcast code blacklist is not empty)\n"
				, CANDIDATE_LITE_ID_CSTR(candidate->lite_), ADS_ENTITY_ID_CSTR(creative->creative_id_), ADS_ENTITY_ID_CSTR(creative->id_)));
			if (candidate->lite_->is_watched()) ctx->log_watched_info(*candidate->lite_, nullptr, creative->id_, "CLEARCAST_CODE_BLACKLIST ");
			applicable = false;
		}
	}

	if (is_source_external)
	{
		auto ad_ext_info = candidate->lite_->ad()->ad_ext_info_;
		if (checked)
			ad_ext_info->check_flags_ |= Ads_Advertisement::EXTERNAL_CLEARCAST_CODE_CHECKED;
		if (!applicable)
			ad_ext_info->check_flags_ |= Ads_Advertisement::EXTERNAL_CLEARCAST_CODE_BANNED;
	}
	return applicable;
}

bool
Ads_Selector::is_creative_applicable_for_slot(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate *candidate,
						const Ads_Creative_Rendition *creative, const Ads_Slot_Base *slot,
						bool ignore_restriction, selection::Error_Code* err, const Profile_Unmatch_Handler &profile_unmatch_handler)
{
	const Ads_Slot_Restriction *slot_restriction = 0;
	Ads_Slot_Restriction *tmp_slot_restriction = 0;
	if (!ignore_restriction)
	{
		if (candidate->restriction(ads::MEDIA_VIDEO))
			slot_restriction = candidate->restriction(ads::MEDIA_VIDEO)->slot_restriction(slot);
		if (candidate->restriction(ads::MEDIA_SITE_SECTION))
		{
			if (!slot_restriction)
				slot_restriction = candidate->restriction(ads::MEDIA_SITE_SECTION)->slot_restriction(slot);
			else if (candidate->restriction(ads::MEDIA_SITE_SECTION)->slot_restriction(slot))
			{
				tmp_slot_restriction = new Ads_Slot_Restriction();
				tmp_slot_restriction->plus(*candidate->restriction(ads::MEDIA_VIDEO)->slot_restriction(slot));
				tmp_slot_restriction->plus(*candidate->restriction(ads::MEDIA_SITE_SECTION)->slot_restriction(slot));
				slot_restriction = tmp_slot_restriction;
			}
		}
	}

	bool ret = this->is_creative_applicable_for_slot(ctx, candidate, creative, slot, slot_restriction, err, profile_unmatch_handler);
	if (tmp_slot_restriction) delete tmp_slot_restriction;

	return ret;
}

#if 0
bool
Ads_Selector::is_rendition_available_for_ad_asset_stores(Ads_Selection_Context *ctx, const Ads_Creative_Rendition *creative)
{
	//only handle content type "external/cablelabs-adi-pid-paid"
	const Ads_GUID adi_content_type_id = ads::entity::make_id(ADS_ENTITY_TYPE_CONTENT_TYPE, 111192);
	if (!creative ||
		(creative->content_type_id_ != adi_content_type_id && creative->primary_content_types_.find(adi_content_type_id) ==  creative->primary_content_types_.end())) return true;

	if (!ctx || !ctx->request_->rep() || ctx->request_->rep()->key_values_.ad_asset_stores_.empty()) return true;

	const Ads_Repository *repo = ctx->repository_;
	Ads_String_List &stores = ctx->request_->rep()->key_values_.ad_asset_stores_;

	bool no_valid_store = true;
	for (Ads_String_List::const_iterator it = stores.begin(); it != stores.end(); ++it)
	{
		Ads_Ad_Asset_Store_Map::const_iterator sit = repo->ad_asset_stores_.find(it->c_str());
		if (sit != repo->ad_asset_stores_.end() && sit->second && sit->second->is_rendition_available(ads::entity::id(creative->id_)))
			return true;
		if (sit != repo->ad_asset_stores_.end()) no_valid_store = false;
	}
	return no_valid_store;
}
#endif

bool
Ads_Selector::is_creative_applicable_for_slot(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate *candidate,
						const Ads_Creative_Rendition *creative, const Ads_Slot_Base *slot,
						const Ads_Slot_Restriction *slot_restriction, selection::Error_Code *err, const Profile_Unmatch_Handler &profile_unmatch_handler)
{
#if defined(ADS_ENABLE_FORECAST)
	if (NULL != creative->rendition_ext_info_ && !candidate->lite_->is_pg_td_ad())
	{
		if (!(ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::BYPASS_RESTRICTION))
			return creative->rendition_ext_info_->profile_check_passed_;
		else
			return true;
	}
#endif
	const Ads_Advertisement_Candidate_Lite *lite = candidate->lite_;
	bool verbose = ctx->verbose_ > 0;

	bool is_scheduled_creative = candidate->is_scheduled_in_slot(slot);

	selection::Error_Code dummy_err;
	if (err == nullptr)
		err = &dummy_err;
	*err = selection::Error_Code::UNDEFINED;

	if (slot->env() == ads::ENV_PAGE || slot->env() == ads::ENV_PLAYER)
	{
		/// width & height
		if (ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::BYPASS_RESTRICTION)
			return true;
		if (!ctx->is_dimension_compatible_for_slot(creative->width_, creative->height_, slot))
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE,
				           "ad %s creative %s wxh of creative %s %dx%d not match %dx%d for slot %s\n",
				           CANDIDATE_LITE_ID_CSTR(lite),
				           ADS_ENTITY_ID_CSTR(creative->creative_id_),
				           CREATIVE_EXTID_CSTR(creative),
				           creative->width_, creative->height_,
				           slot->width_, slot->height_, slot->custom_id_.c_str())
				         );
			if (lite->is_watched())
				ctx->log_watched_info(*lite, slot, creative->id_, "INCOMPATIBLE_DIMENSION");
			*err = selection::Error_Code::INCOMPATIBLE_RENDITION_DIMENSION;
			ctx->mkpl_executor_->log_ad_error(ctx, *lite, selection::Error_Code::INCOMPATIBLE_RENDITION_DIMENSION, selection::Error_Code::PROFILE_CHECK_FAILED);
			return false;
		}
	}
	else
	{
		const Ads_Video_Slot *vslot = dynamic_cast<const Ads_Video_Slot*>(slot);
		if (vslot == nullptr) return false;

		if (!(ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::BYPASS_RESTRICTION))
		{
			/// file size
			if (!is_scheduled_creative && creative->ad_size_ != size_t(-1) && vslot->max_ad_size_ < creative->ad_size_)
			{
				if (verbose)
					ADS_DEBUG((LP_TRACE, "ad %s creative %s file size of creative %s %s exceeds limitation %s for slot %s\n",
					           CANDIDATE_LITE_ID_CSTR(lite),
					           ADS_ENTITY_ID_CSTR(creative->creative_id_),
					           CREATIVE_EXTID_CSTR(creative),
					           ads::i64_to_str(creative->ad_size_).c_str(),
					           ads::i64_to_str(vslot->max_ad_size_).c_str(),
					           slot->custom_id_.c_str()));
				if (lite->is_watched())
					ctx->log_watched_info(*lite, slot, creative->id_, "INCOMPATIBLE_FILE_SIZE " + ctx->restriction_.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_UNIT, -1, true, slot));
				*err = selection::Error_Code::INCOMPATIBLE_RENDITION_FILE_SIZE;
				ctx->mkpl_executor_->log_ad_error(ctx, *lite, selection::Error_Code::INCOMPATIBLE_RENDITION_FILE_SIZE, selection::Error_Code::PROFILE_CHECK_FAILED);
				return false;
			}

			/// file size
			if (!is_scheduled_creative
			    && creative->ad_size_ != size_t(-1)
			    && slot_restriction
			    && slot_restriction->max_individual_ad_size_ < creative->ad_size_)
			{
				if (verbose)
					ADS_DEBUG((LP_TRACE, "ad %s creative %s file size of creative %s %s exceeds limitation %s for slot %s\n",
					           CANDIDATE_LITE_ID_CSTR(lite),
					           ADS_ENTITY_ID_CSTR(creative->creative_id_),
					           CREATIVE_EXTID_CSTR(creative),
					           ads::i64_to_str(creative->ad_size_).c_str(),
					           ads::i64_to_str(slot_restriction->max_individual_ad_size_).c_str(),
					           slot->custom_id_.c_str()));
				if (lite->is_watched() && candidate->restriction(candidate->assoc_))
					ctx->log_watched_info(*lite, slot, creative->id_, "INCOMPATIBLE_FILE_SIZE_RESTRICTION " + candidate->restriction(candidate->assoc_)->info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_UNIT, -1, true, slot));
				*err = selection::Error_Code::INCOMPATIBLE_RENDITION_FILE_SIZE;
				ctx->mkpl_executor_->log_ad_error(ctx, *lite, *err, selection::Error_Code::PROFILE_CHECK_FAILED);
				return false;
			}

			///Live mode do not use ESTIMATE duration interruptive creative for selection
			if (ctx->is_live() && creative->duration_type_ == Ads_Creative_Rendition::ESTIMATE
				&& !ctx->skip_overflow_ad_
				&& slot->pseudo_ad_unit_name_ != "stream_preroll"
				&& slot->pseudo_ad_unit_name_ != "stream_postroll"
				&& ads::standard_ad_unit(vslot->time_position_class_.c_str()) != ads::OVERLAY)
			{
				if (verbose)
					ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s not fit for live slot %s because of duration_type ESTIMATE\n",
					           CANDIDATE_LITE_ID_CSTR(lite),
					           ADS_ENTITY_ID_CSTR(creative->creative_id_),
					           CREATIVE_EXTID_CSTR(creative),
					           slot->custom_id_.c_str()));

				if (lite->is_watched())
					ctx->log_watched_info(*lite, slot, creative->id_, "ESTIMATE_NOT_FOR_LIVE");
				*err = selection::Error_Code::ESTIMATE_RENDITION_DURATION_FOR_LIVE;
				ctx->mkpl_executor_->log_ad_error(ctx, *lite, *err, selection::Error_Code::PROFILE_CHECK_FAILED);
				return false;
			}
		}
		/// duration
		if (!is_scheduled_creative && creative->duration_type_ != Ads_Creative_Rendition::VARIABLE && !candidate->ad_->is_bumper())
		{
			size_t max_duration = std::min(vslot->max_advertisement_duration_, vslot->max_duration_);
			if (creative->duration_ > max_duration)
			{
				if (verbose)
					ADS_DEBUG((LP_TRACE, "ad %s creative %s duration of rendition %s %d exceeds %d for slot %s\n",
					           CANDIDATE_LITE_ID_CSTR(lite),
					           ADS_ENTITY_ID_CSTR(creative->creative_id_),
					           CREATIVE_EXTID_CSTR(creative),
					           creative->duration_,
					           max_duration,
					           slot->custom_id_.c_str()));
				if (lite->is_watched())
					ctx->log_watched_info(*lite, slot, creative->id_, "LARGE_DURATION " + ctx->restriction_.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_UNIT, -1, true, slot));
				*err = selection::Error_Code::LARGE_RENDITION_DURATION;
				ctx->mkpl_executor_->log_ad_error(ctx, *lite, *err, selection::Error_Code::PROFILE_CHECK_FAILED);
				return false;
			}
			if (slot_restriction)
			{
				if (vslot->is_scheduled())
				{
					max_duration = slot_restriction->max_individual_ad_duration_;
				}
				else
				{
					max_duration = std::min(slot_restriction->max_individual_ad_duration_, slot_restriction->max_commercial_break_duration_);
				}
				if (creative->duration_ > max_duration)
				{
					if (verbose)
						ADS_DEBUG((LP_TRACE, "ad %s creative %s duration of rendition %s %d exceeds %d for slot %s\n",
						           CANDIDATE_LITE_ID_CSTR(lite),
						           ADS_ENTITY_ID_CSTR(creative->creative_id_),
						           CREATIVE_EXTID_CSTR(creative),
						           creative->duration_,
						           max_duration,
						           slot->custom_id_.c_str()));
					if (lite->is_watched() && candidate->restriction(candidate->assoc_))
						ctx->log_watched_info(*lite, slot, creative->id_, "LARGE_DURATION_RESTRICTION " + candidate->restriction(candidate->assoc_)->info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_UNIT, -1, true, slot));
					*err = selection::Error_Code::LARGE_RENDITION_DURATION;
					ctx->mkpl_executor_->log_ad_error(ctx, *lite, *err, selection::Error_Code::PROFILE_CHECK_FAILED);
					return false;
				}
			}
		}
		if ((ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::BYPASS_RESTRICTION))
			return true;
	}

#if defined(ADS_ENABLE_FORECAST)
	if (creative->faked_)
	{
		bool pass = true;
		const Ads_Ad_Unit* ad_unit = candidate->ad_unit();
		if (candidate->ad_ && ad_unit)
		{
			pass = false;
			for (std::list<Ads_Slot_Profile *>::const_iterator it = slot->profiles_.begin();
			        it != slot->profiles_.end();
			        ++it)
			{
				const Ads_Slot_Profile *profile = *it;
				if (ads::set_overlapped(
				            profile->base_ad_units_.begin(),
				            profile->base_ad_units_.end(),
				            ad_unit->base_ad_units_.begin(),
				            ad_unit->base_ad_units_.end())
				   )
				{
					pass = true;
					break;
				}
			}
		}

		if (!pass && verbose)
			ADS_LOG((LP_TRACE, "candidate %d faked creative failed in slot %s\n",
						ads::entity::id(candidate->ad_->id_),
						slot->custom_id_.c_str()));
		return pass;
	}
#endif
	/// indicators
	if (!is_scheduled_creative && slot->creative_indicators() > 0)
	{
		if (slot->creative_indicators() & ~creative->indicators_)
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s indicators %08x for slot %s %08x\n",
				           CANDIDATE_LITE_ID_CSTR(lite),
				           ADS_ENTITY_ID_CSTR(creative->creative_id_),
				           CREATIVE_EXTID_CSTR(creative),
				           creative->indicators_,
				           slot->custom_id_.c_str(),
				           slot->creative_indicators()));
			if (lite->is_watched())
				ctx->log_watched_info(*lite, slot, creative->id_, "INCOMPATIBLE_QUALIFICATION " + ctx->restriction_.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_UNIT, -1, true, slot));
			*err = selection::Error_Code::INCOMPATIBLE_RENDITION_QUALIFICATION;
			ctx->mkpl_executor_->log_ad_error(ctx, *lite, *err, selection::Error_Code::PROFILE_CHECK_FAILED);
			return false;
		}
	}

	if (slot_restriction && slot_restriction->creative_indicators() > 0)
	{
		if (slot_restriction->creative_indicators() & ~creative->indicators_)
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s indicators %08x for slot %s %08x\n",
				           CANDIDATE_LITE_ID_CSTR(lite),
				           ADS_ENTITY_ID_CSTR(creative->creative_id_),
				           CREATIVE_EXTID_CSTR(creative), creative->indicators_, slot->custom_id_.c_str(), slot_restriction->creative_indicators()));
			if (lite->is_watched() && candidate->restriction(candidate->assoc_))
				ctx->log_watched_info(*lite, slot, creative->id_, "INCOMPATIBLE_QUALIFICATION_RESTRICTION " + candidate->restriction(candidate->assoc_)->info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_UNIT, -1, true, slot));
			*err = selection::Error_Code::INCOMPATIBLE_RENDITION_QUALIFICATION;
			ctx->mkpl_executor_->log_ad_error(ctx, *lite, *err, selection::Error_Code::PROFILE_CHECK_FAILED);

			return false;
		}
	}

	if (candidate->is_external_creative_api_banned(ctx, slot, creative->creative_api_id_))
	{
		if (verbose)
			ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s creative api %s is banned for slot %s\n",
			           CANDIDATE_LITE_ID_CSTR(lite),
			           ADS_ENTITY_ID_CSTR(creative->creative_id_),
			           CREATIVE_EXTID_CSTR(creative), ADS_ENTITY_ID_CSTR(creative->creative_api_id_), slot->custom_id_.c_str()));
		*err = selection::Error_Code::CREATIVE_API_BANNED;
		return false;
	}

#if 0
	/// flash compatibility check
	if (!ctx->request_->flash_version().empty())
	{
		if (!this->check_creative_rendition_flash_compatibility(ctx, ctx->request_->flash_version(), creative))
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE, "ad %s creative %s rendition %s incompatible flash version %s\n",
				           ADS_ENTITY_ID_CSTR(candidate->ad_->id_),
				           ADS_ENTITY_ID_CSTR(creative->creative_id_),
				           ADS_ENTITY_ID_CSTR(creative->id_), ctx->request_->flash_version().c_str()));
			if (candidate->is_watched()) ctx->log_watch(candidate->ad_->id_, slot, creative->id_, "INCOMPATIBLE_FLASH_VERSION");
			return false;
		}
	}
#endif

	if (lite->is_watched())
		ctx->log_watched_info(*lite, slot, creative->id_, "CREATIVE_FOUND");

	bool skip_profile_match = (candidate->lite_ && (candidate->lite_->is_two_phase_translated_ad() || candidate->lite_->is_pg_td_ad()));
	bool applicable = skip_profile_match || is_creative_applicable_for_profiles(ctx, candidate, creative, slot, *err, profile_unmatch_handler);
	return applicable;
}

void
Ads_Selector::collect_jit_ingestions(Ads_Selection_Context *ctx)
{
	for (const auto &lite_pair : ctx->pre_final_candidates_)
	{
		const auto *lite = lite_pair.second;
		if (lite->is_pg_ad())
			continue;
		add_jit_ingestions_by_creative_check(ctx, lite);
	}
}

void
Ads_Selector::add_jit_ingestions_by_creative_check(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate_Lite *lite)
{
	if (lite->ad_unit() == nullptr || lite->ad_unit()->slot_type_ != Ads_Ad_Unit::VIDEO)
		return;

	bool is_market_ad = lite->is_pre_selection_external_translated();
	Ads_Advertisement_Candidate_Lite::JiTT_Config::Enablement jit_configured_enablement;
	auto jit_enablement = lite->jitt_enablement(is_market_ad ?
							Ads_Advertisement_Candidate_Lite::JiTT_Config::MARKET :
							Ads_Advertisement_Candidate_Lite::JiTT_Config::NON_VAST,
							&jit_configured_enablement);

	if (ctx->repository_->is_cch_adstor_transcode_package(ctx->jitt().transcode_package_id()) && !is_market_ad
			&& lite->is_profile_matching_pass())
		return;

	bool need_watching = (lite->is_watched() && !lite->jit_candidate_creatives_.empty());
	if (!jit_enablement.is_enabled() && !need_watching)
		return;

	if (!jit_enablement.is_enabled())
	{
		ADS_ASSERT(need_watching);
		bool is_valid_package_id = ads::entity::is_valid_id(ctx->jitt().transcode_package_id());
		if (!jit_configured_enablement.is_enabled() && is_valid_package_id)
		{
			ctx->log_watched_info(*lite, "JIT_NOT_ENABLED");
		}
		else if (jit_configured_enablement.is_enabled() && !is_valid_package_id)
		{
			ctx->log_watched_info(*lite, "TRANSCODE_PACKAGE_INVALID");
		}
		return;
	}

	//jit_candidate_creatives_ stores creatives that has all renditions fail profile matching
	for (const auto& pr : lite->jit_candidate_creatives_)
	{
		const Ads_Slot_Base *slot = pr.first;
		const auto& failed_creatives = pr.second;
		if (failed_creatives.empty())
		{
			continue;
		}

		for (const auto* creative : failed_creatives)
		{
			if (is_market_ad)
			{
				if (!(lite->operation_owner().network_.config()->enable_market_creative_replacement_ && lite->external_creative_info(creative) && lite->external_creative_info(creative)->client_renditions_.empty()))
					this->add_wrapper_creative_jit_ingestion(ctx, lite, creative, slot);
			}
			else
			{
				if (need_watching)
				{
					Ads_String msg = "JIT_TASK_IN_PROGRESS -1,-1,-1," + ads::i64_to_str(ctx->jitt().transcode_package_id());
					for (Ads_GUID rendition_id : creative->children_)
						ctx->log_watched_info(*lite, slot, rendition_id, msg);
				}
				ctx->jitt().add_ingestion_for_internal_creative(creative->id_, *lite);
			}
		}
	}
	return;
}

void
Ads_Selector::add_wrapper_creative_jit_ingestion(Ads_Selection_Context* ctx, const Ads_Advertisement_Candidate_Lite* lite, const Ads_Creative_Rendition *creative, const Ads_Slot_Base *slot)
{
	for (const auto& creative_info : lite->external_creative_infos())
	{
		if (creative != nullptr)
		{
			if (creative_info->creative_.get() != creative)
				continue;
		}
		if (!creative_info->jit_renditions_.empty())
			continue;
		if (lite->is_watched() && !lite->is_profile_matching_pass())
		{
			Ads_GUID creative_rendition_id = creative_info->external_renditions_.empty() ? -1 : creative_info->external_renditions_.front()->id_;
			for (const auto &msg : creative_info->jit_watch_infos_)
			{
				ctx->log_watched_info(*lite, slot, creative_rendition_id, msg);
			}
		}
		if (creative_info->cch_key_.empty())
			continue;

		ctx->jitt().required_ingests_.insert(creative_info->cch_key_);
		ADS_DEBUG((LP_TRACE, "add wrapper creative %s with ext_id %d cch_key %s transcode_package_id %d to CCH ingest list\n"
			, ADS_ENTITY_ID_CSTR(creative_info->creative_->id_)
			, creative_info->creative_->ext_id_
			, creative_info->cch_key_.c_str()
			, ctx->jitt().transcode_package_id()));
	}
	return;
}

bool Ads_Selector::test_advertisement_slot_restrictions(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate *candidate,
		const Ads_Slot_Base *slot, int flags, bool verbose)
 {
	bool recoverable = false;
	return test_advertisement_slot_restrictions(ctx, candidate, slot, flags, verbose, recoverable);
}

bool
Ads_Selector::test_advertisement_slot_restrictions(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate *candidate,
		const Ads_Slot_Base *slot, int flags, bool verbose, bool& recoverable)
{
	recoverable = false;
	const Ads_Advertisement_Candidate_Lite *lite = candidate->lite_;
	const Ads_Advertisement *ad = candidate->ad_;
	if (!candidate->slot_ref(const_cast<Ads_Slot_Base *>(slot)))
		return false;

	if (!candidate->valid_)
	{
		if (lite->is_watched())
			ctx->log_watched_info(*lite, slot, "INVALID_CANDIDATE");
		return false;
	}

	if (candidate->flags_ & Ads_Advertisement_Candidate::FLAG_COMPLETED
#if defined(ADS_ENABLE_FORECAST)
			&& !(ctx->request_flags() & Ads_Request::Smart_Rep::BYPASS_BUDGET_CHECK)
#endif
			)
		return false;

	/// cached
	if (!(flags & FLAG_IGNORE_CACHED_RESULT))
	{
		if (candidate->non_applicable_slots_.find(slot) != candidate->non_applicable_slots_.end())
		{
			const_cast<Ads_Advertisement_Candidate_Lite*>(lite)->set_sub_reject_reason_bitmap(Reject_Ads::SLOT_NO_CREATIVE);
			if (lite->is_watched() && candidate->applicable_creatives(slot).empty())
				ctx->log_watched_info(*lite, slot, "NO_CREATIVE");
			return false;
		}
	}

	if (slot->env() == ads::ENV_PAGE || slot->env() == ads::ENV_PLAYER)
	{
//		if (!is_candidate_dimension_compatible_for_slot(ctx, candidate, slot)) return false;
		if (!ctx->is_dimension_compatible_for_slot(ad->width(), ad->height(), slot, false /* exact_only */))
		{
			ADS_DEBUG((LP_TRACE, "ad %s wxh %dx%d not match for slot %s dimensions %s\n",
						 CANDIDATE_LITE_ID_CSTR(lite), ad->width(), ad->height(),
						 slot->custom_id_.c_str(), slot->compatible_dimension_str().c_str())
					 );

			if (lite->is_watched())
				ctx->log_watched_info(*lite, slot, "DIMENSION_UNMATCH");
			return false;
		}

		const Ads_Slot_Restriction *vslot_restriction = 0, *pslot_restriction = 0;
		if (candidate->restriction(ads::MEDIA_VIDEO))
			vslot_restriction = candidate->restriction(ads::MEDIA_VIDEO)->slot_restriction(slot);
		if (candidate->restriction(ads::MEDIA_SITE_SECTION))
			pslot_restriction = candidate->restriction(ads::MEDIA_SITE_SECTION)->slot_restriction(slot);

		/// max # ad
		bool check_companion = true;
		if (ctx->request_->has_explicit_candidates())
			check_companion = ctx->request_->rep()->ads_.check_companion_;

		// workaround for MRM-13396
		size_t max_num_advertisements = std::min(slot->max_num_advertisements_,
				std::min((vslot_restriction ? vslot_restriction->max_ad_number_per_slot_ : 1),
						(pslot_restriction ? pslot_restriction->max_ad_number_per_slot_ : 1)));

		if (max_num_advertisements == 0 && slot->num_advertisements_ >= max_num_advertisements)//zlin_todo:bug or intended?
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE, "ad %s slot %s (MAX_NUMBER_ADS)\n", CANDIDATE_LITE_ID_CSTR(lite), slot->custom_id_.c_str()));
			if (lite->is_watched())
				ctx->log_watched_info(*lite, slot, "MAX_NUM_ADS " + ctx->restriction_.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_UNIT, -1, true, slot));
			return false;
		}

		if (!check_companion || !candidate->is_companion(flags & FLAG_IGNORE_COMPANION_SIZE))
		{
			if (slot->companion_ad_only() || (slot->companion_ad_only_exempt_sponsorship() && !ad->is_sponsorship())
				|| (vslot_restriction && vslot_restriction->companion_ad_only()) || (pslot_restriction && pslot_restriction->companion_ad_only()))
			{
				if (verbose)
					ADS_DEBUG((LP_TRACE, "ad %s slot %s (COMPANION_AD_ONLY)\n", CANDIDATE_LITE_ID_CSTR(lite), slot->custom_id_.c_str()));
				candidate->non_applicable_slots_.insert(slot);
				if (lite->is_watched())
				{
					if (slot->companion_ad_only() || (slot->companion_ad_only_exempt_sponsorship() && !ad->is_sponsorship()))
						ctx->log_watched_info(*lite, slot, "COMPANION_ONLY " + ctx->restriction_.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_UNIT, -1, true, slot));
					else if (vslot_restriction && vslot_restriction->companion_ad_only() && candidate->restriction(ads::MEDIA_VIDEO))
						ctx->log_watched_info(*lite, slot, "COMPANION_ONLY " + candidate->restriction(ads::MEDIA_VIDEO)->info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_UNIT, -1, true, slot));
					else if (pslot_restriction && pslot_restriction->companion_ad_only() && candidate->restriction(ads::MEDIA_SITE_SECTION))
						ctx->log_watched_info(*lite, slot, "COMPANION_ONLY " + candidate->restriction(ads::MEDIA_SITE_SECTION)->info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_UNIT, -1, true, slot));
				}
				return false;
			}
		}
	}
	else
	{
		const Ads_Video_Slot *vslot = dynamic_cast<const Ads_Video_Slot*>(slot);
		if (vslot == nullptr)
			return false;

		const Ads_Slot_Restriction *vslot_restriction = 0;
		if (candidate->restriction(ads::MEDIA_VIDEO))
			vslot_restriction = candidate->restriction(ads::MEDIA_VIDEO)->slot_restriction(slot);
		if (slot->max_num_advertisements_ == 0 || (vslot_restriction && vslot_restriction->max_ad_number_per_slot_ == 0 && !vslot->is_scheduled()))
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE, "ad %s slot %s (MAX_NUMBER_ADS)\n",
				           CANDIDATE_LITE_ID_CSTR(lite),
				           slot->custom_id_.c_str())
				         );
			if (lite->is_watched())
			{
				if (slot->max_num_advertisements_)
					ctx->log_watched_info(*lite, slot, "MAX_NUM_ADS " + ctx->restriction_.info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_UNIT, -1, true, slot));
				else if (candidate->restriction(ads::MEDIA_VIDEO))
					ctx->log_watched_info(*lite, slot, "MAX_NUM_ADS " + candidate->restriction(ads::MEDIA_VIDEO)->info_.to_string(ctx, Ads_Asset_Restriction::RESTRICTION_AD_UNIT, -1, true, slot));
			}
			return false;
		}

		if ((slot->companion_ad_only() || (slot->companion_ad_only_exempt_sponsorship() && !ad->is_sponsorship()))
			&& !candidate->is_companion(flags & FLAG_IGNORE_COMPANION_SIZE))
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE, "ad %s slot %s (COMPANION_AD_ONLY)\n",
						   CANDIDATE_LITE_ID_CSTR(lite),
						   slot->custom_id_.c_str())
						 );
			candidate->non_applicable_slots_.insert(slot);
			if (lite->is_watched())
				ctx->log_watched_info(*lite, slot, "CBP_COMPANION_ONLY");
			lite->log_selection_error(selection::Error_Code::SLOT_COMPANION_AD_ONLY, slot, selection::Error_Code::COMPETITION_FAILURE);
			return false;
		}

		/// ignore non-tracking ads for tracking-only slot
		if (slot->flags_ & Ads_Slot_Base::FLAG_TRACKING_ONLY)
		{
			if (!ad->is_tracking())
			{
				candidate->non_applicable_slots_.insert(slot);
				if (lite->is_watched())
					ctx->log_watched_info(*lite, slot, "TRACKING_ONLY");
				return false;
			}

			(void) candidate->ensure_applicable_creatives(slot);
			return true;
		}

		if (!slot->can_associate(candidate->assoc_))
		{
			if (verbose)
				ADS_DEBUG((LP_TRACE, "ad %s failed for slot %s (ASSOCIATE)\n", CANDIDATE_LITE_ID_CSTR(lite), slot->custom_id_.c_str()));

			candidate->non_applicable_slots_.insert(slot);
			if (lite->is_watched())
				ctx->log_watched_info(*lite, slot, "NO_ASSOCIATE");
			return false;
		}

		if (!(flags & FLAG_IGNORE_POSITION))
		{
			const Ads_Ad_Unit *ad_unit = ad->ad_unit();
			if (ad_unit)
			{
				std::list<Ads_Advertisement_Candidate *>::iterator dummy;
				if (!const_cast<Ads_Video_Slot *>(vslot)->find_position_for_advertisement(ctx, candidate, dummy, verbose, recoverable))
					return false;

				// check whether position in slot has been occupied
				if (ad_unit->position_in_slot_ != 0)
				{
					int position_occupied = 0;
					if (ad_unit->position_in_slot_ == -1)
					{
						if (vslot->masks_ & Ads_Video_Slot::MASK_LAST_POSITION)
							position_occupied = ad_unit->position_in_slot_;

						if (vslot->pod_sequence_ != -1 && vslot->total_pods_in_group_ != -1
								&& vslot->pod_sequence_ != vslot->total_pods_in_group_)//scte: last pod in group
						{
							const_cast<Ads_Advertisement_Candidate_Lite*>(lite)->set_sub_reject_reason_bitmap(Reject_Ads::SLOT_POD_OCCUPIED);
							ADS_DEBUG((LP_TRACE, "SCTE position in pod targeting: ad %s (position: %d),  slot %s (breakOpportunitySequence %d, breakOpportunitiesExpected: %d) unmatch\n", CANDIDATE_LITE_ID_CSTR(lite), ad_unit->position_in_slot_, slot->custom_id_.c_str(), vslot->pod_sequence_, vslot->total_pods_in_group_));
								if (lite->is_watched())
									ctx->log_watched_info(*lite, slot, "POD_OCCUPIED");
							return false;
						}
					}
					else if (ad_unit->position_in_slot_ > 0 && ad_unit->position_in_slot_ <= Ads_Video_Slot::MAX_POSITION)
					{
						position_occupied = ad_unit->position_in_slot_;
						for (int i = ad_unit->position_in_slot_; i > 0; --i)
						{
							if (!(vslot->masks_ & (Ads_Video_Slot::MASK_FIRST_POSITION << (i - 1))))
							{
								position_occupied = 0;
								break;
							}
						}

						if (vslot->pod_sequence_ != -1 //scte-130 position-in-pod targeting
								&& ad_unit->position_in_slot_ != vslot->pod_sequence_)
						{
							const_cast<Ads_Advertisement_Candidate_Lite*>(lite)->set_sub_reject_reason_bitmap(Reject_Ads::SLOT_POD_OCCUPIED);
							ADS_DEBUG((LP_TRACE, "SCTE position-in-pod targeting: ad  %s (position: %d),  slot %s (breakOpportunitySequence %d) unmatch\n", CANDIDATE_LITE_ID_CSTR(lite), ad_unit->position_in_slot_, slot->custom_id_.c_str(), vslot->pod_sequence_));
								if (lite->is_watched())
									ctx->log_watched_info(*lite, slot, "POD_OCCUPIED");
							return false;
						}
					}

					if (position_occupied)
					{
						if (verbose)
							ADS_DEBUG((LP_TRACE, "ad %s slot %s (POSITION_OCCUPIED %d)\n",
								CANDIDATE_LITE_ID_CSTR(lite), slot->custom_id_.c_str(), position_occupied));

						if (lite->is_watched())
							ctx->log_watched_info(*lite, slot, "POD_OCCUPIED");

						lite->log_selection_error(selection::Error_Code::POSITION_OCCUPIED, slot, selection::Error_Code::COMPETITION_FAILURE);
						return false;
					}
				}
			}
		}

		// compliance info validation for inventory protection
		selection::Error_Code error_code = selection::Error_Code::NO_ERROR;
		if (!candidate->check_compliance_for_inventory_protection(ctx, error_code))
			return false;
	}

#if defined(ADS_ENABLE_FORECAST)
	//if (unconstrained)
	if (ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::BYPASS_RESTRICTION)
	{
		return true;
	}

#endif

	if (flags & FLAG_IGNORE_CREATIVE)
		return true;

	//need to keep this at the bottom of this function for variable ad to by pass restriction
	//if in LIVE mode w none stream preroll/postroll slot, check for guaranteed creative
	//if in LIVE mode w stream preroll/postroll slot, check for non-variable creative
	//if in on demand mode, check for non-variable creative
	bool has_guaranteed = true;
	bool variable_only = false;
	bool is_stream_slot =  false;

	if (slot->env() == ads::ENV_VIDEO)
	{
		const Ads_Video_Slot* vslot = reinterpret_cast<const Ads_Video_Slot*>(slot);
		is_stream_slot = (vslot->pseudo_ad_unit_name_ == "stream_preroll" || vslot->pseudo_ad_unit_name_ == "stream_postroll"
				|| ads::standard_ad_unit(vslot->time_position_class_.c_str()) == ads::OVERLAY);
		has_guaranteed = false;
		variable_only = true;
		for (Creative_Rendition_List::const_iterator it = candidate->applicable_creatives(slot).begin();
				it != candidate->applicable_creatives(slot).end(); ++it)
		{
			const Ads_Creative_Rendition *creative = *it;
			if (creative->duration_type_ == Ads_Creative_Rendition::GUARANTEED)
			{
				has_guaranteed = true;
				variable_only = false;
				break;
			}
			else if (creative->duration_type_ == Ads_Creative_Rendition::ESTIMATE)
			{
				variable_only = false;
				if (!ctx->is_live())
					break;
			}
		}

		if (ctx->is_live() && !has_guaranteed && !is_stream_slot && !ctx->skip_overflow_ad_)
		{
			ADS_DEBUG((LP_TRACE, "ad %s failed due to no GUARANTEED creative in Live mode\n", CANDIDATE_LITE_ID_CSTR(lite)));
			if (lite->is_watched())
				ctx->log_watched_info(*lite, slot, "NO_GUARANTEED_CREATIVE_FOR_LIVE");
		}
		else if (variable_only)
		{
			ADS_DEBUG((LP_TRACE, "ad %s failed due to no GUARANTEED/ESTIMATE creative\n", CANDIDATE_LITE_ID_CSTR(lite)));
			if (lite->is_watched())
				ctx->log_watched_info(*lite, slot, "NO_APPLICABLE_CREATIVE");
		}
	}

	return (has_guaranteed || is_stream_slot || !ctx->is_live() || ctx->skip_overflow_ad_) && !variable_only;
}

/**
 * find first applicable creative ids which is in flight date range
 * @param creative_set_precedence
 * @param creative_set
 * @param ad_id
 * @param time_now
 * @param ad_owner_terms
 * @param applicable_creative_ids output
 * @return 0 for success
 */
int Ads_Selector::find_applicable_creative_ids_by_precedence(int creative_set_precedence, const Ads_Advertisement::Creative_Audience_Set &creative_set
	, Ads_GUID ad_id, time_t time_now, const Ads_GUID_Set &ad_owner_terms, Ads_GUID_Vector &applicable_creative_ids)
{
	if (creative_set_precedence != DEFAULT_SET_PRECEDENCE_IN_REPA) //the precedence of default set is INT_MAX and default set has no audience item
	{
		if (creative_set.audience_item_ == nullptr)
		{
			ADS_LOG((LP_ERROR, "ad[%s] creative set[%d]: audience item nullptr, precedence: %d\n", ADS_ENTITY_ID_CSTR(ad_id), creative_set.id_, creative_set_precedence));
			return -1;
		}
		if (ad_owner_terms.find(creative_set.audience_item_->term()) == ad_owner_terms.end())
		{
			ADS_DEBUG((LP_DEBUG, "ad[%s] creative set[%d]: failed to find ad owner audience item %s\n", ADS_ENTITY_ID_CSTR(ad_id), creative_set.id_, ADS_ENTITY_ID_CSTR(creative_set.audience_item_->term())));
			return -1;
		}
	}
	for (const auto &creative_unit : creative_set.creative_units_)
	{
		const auto &creative_flight_date = creative_unit.second;
		if ((creative_flight_date.first != 0 && creative_flight_date.first > time_now) ||
			(creative_flight_date.second != 0 && creative_flight_date.second < time_now))
		{
			ADS_DEBUG((LP_DEBUG, "ad[%s] creative set[%d]: creative[%s] flight window not match\n", ADS_ENTITY_ID_CSTR(ad_id), creative_set.id_, ADS_ENTITY_ID_CSTR(creative_unit.first)));
			continue;
		}
		Ads_GUID creative_id = creative_unit.first;
		ADS_DEBUG((LP_DEBUG, "ad[%s] creative set[%d] found valid creative:%s\n", ADS_ENTITY_ID_CSTR(ad_id), creative_set.id_, ADS_ENTITY_ID_CSTR(creative_id)));
		applicable_creative_ids.push_back(creative_id);
	}
	if (applicable_creative_ids.empty())
		return -1;
	return 0;
}

/**
 * Based on applicable_renditions, output applicable renditions in default set
 * @param ad
 * @param time_now
 * @param applicable_renditions
 * @param default_set_renditions
 * @return 0 for succeed, -1 for failed
 */
//TODO: MOVE to Ads_Scheduler.cpp
int
Ads_Selector::collect_audience_default_set_creative_renditions(const Ads_Advertisement *ad, const time_t &time_now, const Creative_Rendition_List &applicable_renditions, Creative_Rendition_List &default_set_renditions)
{
	Creative_Rendition_List tmp_renditions;
	if (ad == nullptr)
	{
		default_set_renditions.swap(tmp_renditions);
		ADS_LOG((LP_ERROR, "ad is nullptr, not found any applicable creative passed audience targeting check\n"));
		return -1;
	}
	if (applicable_renditions.empty())
	{
		ADS_DEBUG((LP_DEBUG, "applicable_renditions is empty, not found creatives passed audience targeting check.\n"));
		return -1;
	}

	// output default set creative rendition id
	const auto default_set_it = ad->creative_audience_sets_.find(DEFAULT_SET_PRECEDENCE_IN_REPA);
	if (default_set_it != ad->creative_audience_sets_.end())
	{
		Ads_GUID_Vector default_set_creative_ids;
		Ads_GUID_Set faked_ad_owner_terms;
		if (find_applicable_creative_ids_by_precedence(default_set_it->first, default_set_it->second, ad->id_, time_now, faked_ad_owner_terms, default_set_creative_ids) < 0)
		{
			ADS_DEBUG((LP_DEBUG, "failed to find default set renditions\n"));
			return -1;
		}

		for (const auto rendition : applicable_renditions)
		{
			if (rendition == nullptr || find(default_set_creative_ids.begin(), default_set_creative_ids.end(), rendition->creative_id_) == default_set_creative_ids.end())
				continue;
			tmp_renditions.emplace_back(rendition);
			ADS_DEBUG((LP_DEBUG, "ad[%s] found default set creative id: %s, rendition id: %s\n", ADS_ENTITY_ID_CSTR(ad->id_), ADS_ENTITY_ID_CSTR(rendition->creative_id_), ADS_ENTITY_ID_CSTR(rendition->id_)));
		}
	}
	default_set_renditions.swap(tmp_renditions);
	if (default_set_renditions.empty())
		return -1;
	return 0;
}

/**
 * Filter applicable_renditions by creative audience targeting check
 * , and return filtered applicable_renditions, selected_creative_set_precedence, creative_audience_term_id_hit
 * @param ad : ad to do creative audience targeting
 * @param ad_owner : to collect audience terms
 * @param time_now : to check creative set's flight window
 * @param applicable_renditions : input&output. Will return renditions which could pass creative audience targeting
 * 		Note that only one creative set will be selected, however since one creative may contains multiple creative renditions
 * @param selected_creative_set_precedence : output, return selected creative renditions' creative set precedence
 * @param creative_audience_term_id_hit : output, return selected creative set's audience term id
 * @return 0 for succeed
 */
int
Ads_Selector::collect_audience_targetable_creative_renditions(const Ads_Advertisement *ad, const selection::Advertisement_Owner &ad_owner, const time_t &time_now,
		Creative_Rendition_List &applicable_renditions, int &selected_creative_set_precedence, Ads_GUID &creative_audience_term_id_hit)
{
	creative_audience_term_id_hit = ads::entity::invalid_id(ADS_ENTITY_TYPE_TARGETING_TERM);
	Creative_Rendition_List tmp_renditions;
	if (ad == nullptr)
	{
		applicable_renditions.swap(tmp_renditions);
		ADS_LOG((LP_ERROR, "ad is nullptr, not found any applicable creative passed audience targeting check\n"));
		return -1;
	}
	if (applicable_renditions.empty())
	{
		ADS_DEBUG((LP_DEBUG, "applicable_renditions is empty, not found creatives passed audience targeting check.\n"));
		return -1;
	}

	// find applicable renditions with the highest priority set
	for (const auto &ordered_creative_set : ad->creative_audience_sets_) //order by precedence asc, lower precedence value has higher priority
	{
		int creative_set_precedence = ordered_creative_set.first;
		const Ads_Advertisement::Creative_Audience_Set &creative_set = ordered_creative_set.second;
		Ads_GUID_Vector applicable_creative_ids;
		if (find_applicable_creative_ids_by_precedence(creative_set_precedence, creative_set, ad->id_, time_now, ad_owner.terms_, applicable_creative_ids) < 0)
		{
			continue;
		}
		// find all applicable renditions of this set
		for (const auto rendition : applicable_renditions)
		{
			if (rendition == nullptr || find(applicable_creative_ids.begin(), applicable_creative_ids.end(), rendition->creative_id_) == applicable_creative_ids.end())
				continue;
			tmp_renditions.push_back(rendition);
			ADS_DEBUG((LP_DEBUG, "ad[%s] creative set[%d]: creative[%s] rendition [%s] pass creative audience targeting check\n",
				ADS_ENTITY_ID_CSTR(ad->id_), creative_set.id_, ADS_CREATIVE_EXT_ID_CSTR(rendition->creative_id_), CREATIVE_EXTID_CSTR(rendition)));
		}
		if (!tmp_renditions.empty())
		{
			if (creative_set_precedence != DEFAULT_SET_PRECEDENCE_IN_REPA)
			{
				creative_audience_term_id_hit = creative_set.audience_item_->term();
			}
			selected_creative_set_precedence = creative_set_precedence;
			break; // only return applicable renditions of exactly one set
		}
	}
	applicable_renditions.swap(tmp_renditions);
	if (applicable_renditions.empty())
		return -1;
	return 0;
}

int
Ads_Selector::choose_creative_rendition_for_advertisement(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate *candidate, const Ads_Slot_Base *slot, const Ads_Creative_Rendition *&creative_rendition, Ads_GUID driven_id, const Creative_Rendition_List& candidate_renditions)
{
	// Note: "!candidate->is_scheduled_in_slot(slot)" is needed, because:
	// an ad/candidate can be scheduled multiple times using different creative renditions in a slot, if the current schedule instance wants auto select creative_rendition
	// we shouldn't use previous selected creative_rendition in the same slot.
	if (candidate->creative(slot) && !candidate->is_scheduled_in_slot(slot))
	{
		creative_rendition = candidate->creative(slot);
		return 0;
	}

	if (!ctx) return -1;

	const Ads_Advertisement_Candidate_Lite *lite = candidate->lite_;
	const Ads_Advertisement *ad = candidate->ad_;
	const Ads_Advertisement *rotation_ad = (candidate->need_syncing_creative() ? ad->placement_ : ad);
	Ads_Advertisement::ROTATION_TYPE rotation_type = rotation_ad->rotation_type_;

	Ads_GUID_Set rotation_candidates;
	std::map<Ads_GUID, Creative_Rendition_List > renditions;

	bool need_syncing_creative = candidate->need_syncing_creative();
	Ads_GUID syncing_set = -1;

	// PUB-226 ADS - don't serve creatives out of their flight duration that set in syncing set
	// all_creative_in_syncing_set: creatives that belong to some syncing set.
	// eligible_creative_in_syncing_set: creatives that belong to some syncing set which flight date is eligible
	ads::hash_set<Ads_GUID> all_creative_in_syncing_set;
	ads::hash_set<Ads_GUID> eligible_creative_in_syncing_set;
	if (need_syncing_creative)
	{
		Ads_GUID_Map::const_iterator sit = ctx->placement_syncing_sets_.find(rotation_ad->id_);
		if (sit != ctx->placement_syncing_sets_.end())
			syncing_set = sit->second;

		const Ads_Repository *repo = ctx->repository_;
		const Ads_Network *network = 0;
		if (repo->find_network(candidate->get_network_id(), network) >= 0 && network && network->config()->check_syncing_set_flighting_date_)
		{
			for (const auto& element : rotation_ad->creative_syncing_sets_)
			{
				auto& creative_syncing_set = element.second;
				auto uit = creative_syncing_set.find(driven_id);
				if (uit == creative_syncing_set.end()) continue;
				auto* syncing_unit = &uit->second;
				auto ait = syncing_unit->find(candidate->ad_->id_);
				if (ait == syncing_unit->end()) continue;
				if (ctx->in_date_range(creative_syncing_set.start_date_, creative_syncing_set.end_date_))
				{
					eligible_creative_in_syncing_set.insert(ait->second);
				}
				all_creative_in_syncing_set.insert(ait->second);
			}
		}

		if (!ads::entity::is_valid_id(syncing_set))
		{
		for (Ads_Advertisement::Creative_Syncing_Map::const_iterator it = rotation_ad->creative_syncing_sets_.begin();
			 it != rotation_ad->creative_syncing_sets_.end(); ++it)
		{
			const Ads_Advertisement::Creative_Syncing_Set& syncing_set = it->second;
			if (ctx->in_date_range(syncing_set.start_date_, syncing_set.end_date_))
			{
				if (ctx->explicit_renditions_.empty() || candidate->is_scheduled_in_slot(slot))
					rotation_candidates.insert(it->first);
				else
				{
					bool satisfied = true;
					for (Ads_Advertisement::Creative_Syncing_Set::const_iterator uit = syncing_set.begin();
						 uit != syncing_set.end(); ++uit)
					{
						//Ads_GUID driven_id = uit->first;
						const Ads_GUID_RMap* syncing_unit = &uit->second;

						for (Ads_GUID_RMap::const_iterator it = syncing_unit->begin();
							 it != syncing_unit->end(); ++it)
						{
							Ads_GUID ad_id = it->first, creative_id = it->second;
							std::multimap<Ads_GUID, const Ads_Creative_Rendition *>::const_iterator lower = ctx->explicit_renditions_.lower_bound(ad_id)
								, upper = ctx->explicit_renditions_.upper_bound(ad_id);

							if (lower == upper) continue;

							bool found = false;
							for (; lower != upper; ++lower)
							{
								const Ads_Creative_Rendition *rendition = lower->second;
								if (creative_id == rendition->creative_id_)
								{
									found = true;
									break;
								}
							}

							if (!found)
							{
								satisfied = false;
								break;
							}
						}

						if (!satisfied)
							break;
					}

					if (satisfied)
						rotation_candidates.insert(it->first);
				}
			}
		}

		/// fallback to non-syncing mode if no applicable set.
		if (rotation_candidates.empty())
		{
			need_syncing_creative = false;
			ADS_DEBUG((LP_TRACE, "no applicable creative_syncing_set found for ad %s, will fallback to non-syncing mode\n", ADS_ENTITY_ID_CSTR(rotation_ad->id_)));
		}
		}
	}

	if (!need_syncing_creative)
	{
	for (Creative_Rendition_List::const_iterator it = candidate_renditions.begin();
	        it != candidate_renditions.end();
	        ++it)
	{
		const Ads_Creative_Rendition *rendition = *it;
		if (candidate->assoc_ == ads::MEDIA_VIDEO && rendition->duration_type_ == Ads_Creative_Rendition::VARIABLE) continue;
		Ads_GUID id = 0;
		if (all_creative_in_syncing_set.find(rendition->creative_id_) != all_creative_in_syncing_set.end() && eligible_creative_in_syncing_set.find(rendition->creative_id_) == eligible_creative_in_syncing_set.end()) continue;
		if(candidate->ad_->price_type_ == Ads_Advertisement::FIXED)
		{
			id = rendition->creative_id_;
			renditions[rendition->creative_id_].push_back(rendition);
		}
		else
		{
			id = rendition->id_;
			renditions[rendition->id_].push_back(rendition);
		}
		rotation_candidates.insert(id);
	}
	}

	Ads_GUID selected_candidate = syncing_set;

	if (!need_syncing_creative || !ads::entity::is_valid_id(syncing_set))
	{
	if (rotation_candidates.empty()) return -1;

	switch (rotation_type)
	{
	case Ads_Advertisement::WEIGHTED_RANDOM:
	{
		size_t seed = ctx->rand();
		ADS_DEBUG((LP_DEBUG, "ad %s, using seed %s for WEIGHTED_RANDOM\n", CANDIDATE_LITE_ID_CSTR(lite), ads::i64_to_str(seed).c_str()));
		select_creative_based_rotation_factor(rotation_candidates, *rotation_ad, seed, selected_candidate);
		break;
	}
	case Ads_Advertisement::WEIGHTED_EQUAL:
	{
		size_t seed = ctx->rand();
		ADS_DEBUG((LP_DEBUG, "ad %s, using seed %s for WEIGHTED_EQUAL\n", CANDIDATE_LITE_ID_CSTR(lite), ads::i64_to_str(seed).c_str()));
		select_creative_based_equal_weight(rotation_candidates, seed, selected_candidate);
		break;
	}
	case Ads_Advertisement::WEIGHTED_STICKY:
	{
		size_t seed = ctx->request_->id_repo_.iid();
		ADS_DEBUG((LP_DEBUG, "ad %s, using seed %s for WEIGHTED_STICKY\n", CANDIDATE_LITE_ID_CSTR(lite), ads::i64_to_str(seed).c_str()));
		select_creative_based_rotation_factor(rotation_candidates, *rotation_ad, seed, selected_candidate);
		break;
	}
	case Ads_Advertisement::SEQUENCE:
	{
		int sequence =  0;
		std::map<Ads_GUID, int>::const_iterator seq_it = ctx->advertisement_creative_sequence_.find(rotation_ad->id_);
		if (seq_it == ctx->advertisement_creative_sequence_.end())
			ctx->user_->get_last_view_creative_sequence(rotation_ad->id_, sequence);
		else
			sequence = seq_it->second;

		int min_sequence = -1, upper_sequence = -1;
		Ads_GUID_Set::const_iterator min_candidate = rotation_candidates.end(), upper_candidate = rotation_candidates.end();
		for (Ads_GUID_Set::const_iterator it=rotation_candidates.begin();
		        it != rotation_candidates.end();
		        ++it)
		{
			int rf = rotation_ad->rotation_factor(*it);
			if (rf < 0) continue;

			if (rf == sequence)
				continue;

			// minumal sequence
			if (min_sequence < 0 ||  min_sequence > rf)
			{
				min_sequence = rf;
				min_candidate = it;
			}

			if (rf > sequence)
			{
				if (upper_sequence < 0 ||  upper_sequence > rf)
				{
					upper_sequence = rf;
					upper_candidate = it;
				}
			}
		}

		if (upper_candidate != rotation_candidates.end())
		{
			selected_candidate = *upper_candidate;
		}
		else if (min_candidate != rotation_candidates.end())
		{
			selected_candidate = * min_candidate;
		}

		break;
	}
	case Ads_Advertisement::AUDIENCE_WEIGHTED_EQUAL:
	{
		size_t seed = ctx->rand();
		ADS_DEBUG((LP_DEBUG, "ad %s, using seed %s for AUDIENCE_TARGETING\n", CANDIDATE_LITE_ID_CSTR(lite), ads::i64_to_str(seed).c_str()));
		select_creative_based_equal_weight(rotation_candidates, seed, selected_candidate);
		break;
	}
	default:
		ADS_ASSERT(0);
	}

	if (!ads::entity::is_valid_id(selected_candidate))
	{
		selected_candidate = *(rotation_candidates.begin());
	}

	if (need_syncing_creative && ctx->placement_syncing_sets_.find(rotation_ad->id_) == ctx->placement_syncing_sets_.end())
		ctx->placement_syncing_sets_[rotation_ad->id_] = selected_candidate;
	}

	ADS_DEBUG((LP_TRACE, "using %s %s as selected candidate slot:%s[%d], %s\n", (need_syncing_creative ? "syncing set" : "creative_rendition"), ADS_CREATIVE_EXT_ID_CSTR(selected_candidate), ads::env_name(slot->env()), slot->position_, slot->custom_id_.c_str()));

	if (need_syncing_creative)
	{
		Ads_Advertisement::Creative_Syncing_Map::const_iterator sit = rotation_ad->creative_syncing_sets_.find(selected_candidate);
		ADS_ASSERT(sit != rotation_ad->creative_syncing_sets_.end());

		const Ads_Advertisement::Creative_Syncing_Set& syncing_set = sit->second;

				Ads_GUID_RMap dummy;

				const Ads_GUID_RMap* syncing_unit = &dummy;
				Ads_Advertisement::Creative_Syncing_Set::const_iterator uit = syncing_set.find(driven_id);
				if (uit != syncing_set.end())
					syncing_unit = &uit->second;

				Ads_GUID creative_id = -1;
					Ads_GUID_RMap::const_iterator cit = syncing_unit->find(candidate->ad_->id_);
					if (cit != syncing_unit->end())
						creative_id = cit->second;

			//creative_id == 0 means No Creative being selected
			if (creative_id == 0) return 0;

			std::map<Ads_GUID, Creative_Rendition_List > renditions;
			for (Creative_Rendition_List::const_iterator rit = candidate_renditions.begin();
				 rit != candidate_renditions.end(); ++rit)
			{
				const Ads_Creative_Rendition *rendition = *rit;
				if (candidate->assoc_ == ads::MEDIA_VIDEO && rendition->duration_type_ == Ads_Creative_Rendition::VARIABLE) continue;
				if (all_creative_in_syncing_set.find(rendition->creative_id_) != all_creative_in_syncing_set.end() && eligible_creative_in_syncing_set.find(rendition->creative_id_) == eligible_creative_in_syncing_set.end()) continue;
				renditions[rendition->creative_id_].push_back(rendition);
			}

			if (candidate_renditions.empty() || renditions.empty())
            {
				ADS_DEBUG((LP_TRACE, "no applicable creative_rendition for ad %s in slot %s\n", CANDIDATE_LITE_ID_CSTR(lite), slot->custom_id_.c_str()));
				return -1;
            }

			if (ads::entity::is_valid_id(creative_id) && renditions.find(creative_id) != renditions.end())
				creative_rendition = Ads_Selector::preferred_rendition(ctx, candidate, renditions[creative_id]);

			if (!creative_rendition)
			{
				// use random creative_rendition
				size_t seed = ctx->rand();
				ADS_DEBUG((LP_TRACE, "ad %s, using seed %s for RANDOM_CREATIVE in syncing set\n", CANDIDATE_LITE_ID_CSTR(lite), ads::i64_to_str(seed).c_str()));

				size_t i = seed % renditions.size();
				std::map<Ads_GUID, Creative_Rendition_List >::const_iterator rit = renditions.begin();
				while (i > 0 && rit != renditions.end())
				{
					--i;
					++rit;
				}

				if (rit != renditions.end())
					creative_rendition = Ads_Selector::preferred_rendition(ctx, candidate, rit->second);
				else
					creative_rendition = Ads_Selector::preferred_rendition(ctx, candidate, renditions.begin()->second);
			}

		if (rotation_type == Ads_Advertisement::SEQUENCE)
		{
			int rf = rotation_ad->rotation_factor(selected_candidate);
			if (rf >= 0) ctx->user_->add_creative_view_record(ctx->request_->time_created().sec(), rotation_ad->id_, rf);
		}
	}
	else
	{
		creative_rendition = Ads_Selector::preferred_rendition(ctx, candidate, renditions[selected_candidate]);
		if (!creative_rendition)
			return 0;

		Ads_Selector::update_none_syncing_creative_view_record(ctx, candidate, creative_rendition);
	}

	if(candidate->ad_->faked_)
	{
		Ads_String tpc = candidate->ad_->ad_unit_->time_position_class_.c_str();
		// For SSP Non-linear ad, ClickThrough is in <NonLinear> node.
		// For SSP companion ad, ClickThrough & TrackingEvent are both in <Companion> node
		if(tpc.compare("overlay") ==0 || tpc.compare("display") ==0)
		{
			Ads_Advertisement::Creative_Action *rendition_action = Ads_Translator_Base::get_creative_action(
							ads::creative::make_ext_id(creative_rendition->id_, creative_rendition->ext_id_), (Ads_Advertisement*)(candidate->ad_), ctx);
			if (creative_rendition->creative_)
			{
				Ads_Advertisement::Creative_Action *creative_action = Ads_Translator_Base::get_creative_action(
							ads::creative::make_ext_id(creative_rendition->creative_->id_, creative_rendition->creative_->ext_id_), (Ads_Advertisement*)(candidate->ad_), ctx);

				//copy selected rendition click through action into creative_rendition action
				if (creative_rendition->rendition_ext_info_ && !creative_rendition->rendition_ext_info_->actions_used_)
				{
					for (size_t i = 0; i < Ads_Creative_Rendition::URL_LAST; i++)
					{
						std::copy(rendition_action->actions_[i].begin(), rendition_action->actions_[i].end(), std::inserter(creative_action->actions_[i], creative_action->actions_[i].begin()));
					}
					creative_rendition->rendition_ext_info_->actions_used_ = true;
				}
			}
		}
		if(tpc.compare("display") ==0 )
		{
			// cloned rendition A & B are related if they are from the same <CompanionAd> node in VAST response.
			// If rendition A is selected this time for companion ad-1,
			// then set rendition A related rendition B rendition_used_ = true, B will not be used for companion ad-2.
			if (creative_rendition && creative_rendition->rendition_ext_info_ && !creative_rendition->rendition_ext_info_->related_renditions_.empty())
			{
				for (std::set<Ads_Creative_Rendition*>::const_iterator ait = creative_rendition->rendition_ext_info_->related_renditions_.begin();
					ait != creative_rendition->rendition_ext_info_->related_renditions_.end();
					++ait)
				{
					Ads_Creative_Rendition *rendition = *ait;
					if (rendition->rendition_ext_info_)
						rendition->rendition_ext_info_->rendition_used_ = true;
				}
			}
		}
	}

	return 0;
}

int
Ads_Selector::select_creative_based_equal_weight(const Ads_GUID_Set &rotation_creative_candidates, size_t seed, Ads_GUID &selected_creative_candidate)
{
	size_t i = seed % rotation_creative_candidates.size();
	Ads_GUID_Set::const_iterator cit = rotation_creative_candidates.begin();
	while (i > 0 && cit != rotation_creative_candidates.end())
	{
		--i;
		++cit;
	}
	if (cit != rotation_creative_candidates.end())
	{
		selected_creative_candidate = *cit;
	}
	return 0;
}

// VCBS creative dark period, select a creative based on weighting in a flight date set
int
Ads_Selector::select_creative_in_flight_date_by_weighting(const Ads_Advertisement::Creative_Weight_List& rotation_creative_candidates, size_t& seed, Ads_GUID& selected_creative)
{
	size_t sum = 0;
	int rf = -1;
	for (const auto& rotation_creative : rotation_creative_candidates)
	{
		rf = rotation_creative.second;
		if (rf < 0)
			continue;
		sum += rf;
	}

	if (sum == 0)
	{
		ADS_DEBUG((LP_DEBUG, "zero sum. error?\n"));
		return -1;
	}
	size_t i = seed % sum;
	sum = 0;
	rf = -1;
	for (const auto& rotation_creative : rotation_creative_candidates)
	{
		rf = rotation_creative.second;
		if (rf < 0)
			continue;
		sum += rf;
		if (i < sum)
		{
			selected_creative = rotation_creative.first;
			break;
		}
	}
	return 0;
}

int
Ads_Selector::select_creative_based_rotation_factor(const Ads_GUID_Set &rotation_creative_candidates, const Ads_Advertisement &rotation_ad, size_t seed,
	Ads_GUID &selected_creative_candidate)
{
	size_t sum = 0;
	int rf = -1;
	for (const auto &rotation_creative : rotation_creative_candidates)
	{
		rf = rotation_ad.rotation_factor(rotation_creative);
		if (rf < 0)
			continue;
		sum += rf;
	}

	if (sum == 0)
	{
		ADS_DEBUG((LP_DEBUG, "zero sum. error?\n"));
		return -1;
	}

	size_t i = seed % sum;
	sum = 0;
	rf = -1;

	for (const auto &rotation_creative : rotation_creative_candidates)
	{
		rf = rotation_ad.rotation_factor(rotation_creative);
		if (rf < 0)
			continue;
		sum += rf;
		if (i < sum)
		{
			selected_creative_candidate = rotation_creative;
			break;
		}
	}
	return 0;
}

int
Ads_Selector::calculate_revenue_share(const Ads_Repository *repo, ads::MEDIA_TYPE assoc, Ads_GUID network_from, Ads_GUID network_to, Ads_Network::REVENUE_SHARE_TYPE type, int64_t revenue, int64_t &share)
{
#if defined(ADS_ENABLE_FORECAST)
	return 0;//suppress warning
#endif
	const Ads_Network *network = 0;
	REPO_FIND_ENTITY_RETURN(repo, network, network_from, network, -1);

	Ads_Revenue_Share_Factor rsf = network->revenue_share_factor(network_to, assoc, type);
	share = 0;

	if (rsf.second >= 0)
		share = rsf.second;
	else
		share = 0;

	if (rsf.first >= 0)
	{
		share = std::max(ADS_APPLY_MAGNIFIED_PERCENTAGE(revenue, rsf.first), rsf.second);
	}

	return 0;
}

int
Ads_Selector::update_rule_counter_for_wasted_inventory(Ads_Selection_Context *ctx)
{
	std::set<Ads_Slot_Base*> used_slots;
	for (const auto ref : ctx->delivered_candidates_)
	{
		const auto slot = ref->slot_;
		if (ref->candidate_ && (ref->candidate_->flags_ & Ads_Advertisement_Candidate::FLAG_AF_NOT_UPDATE_RULE_COUNTER) == 0 )
		{
			used_slots.insert(slot);
		}
	}
	//AF will not update counter, just update callback_counter in bin log.
	/// update opportunity counters for slots not in use (waste of inventory)
	Ads_GUID_Pair_Set unused_rules/*chance rule in empty slots*/, used_rules/*chance rule in used slots*/, win_rules/*HG rules*/;
	for (Ads_Slot_List::iterator sit = ctx->request_slots_.begin(); sit != ctx->request_slots_.end(); ++sit)
	{
		Ads_Slot_Base *slot = *sit;

		///TODO: escape criteria
		if (slot->flags_ & Ads_Slot_Base::FLAG_TRACKING_ONLY) continue;
		bool used = (slot->num_advertisements_ > 0);
		if (used_slots.find(slot) == used_slots.end())
			used = false;

		for (Ads_Asset_Section_Closure_Map::iterator it = ctx->closures_.begin(); it!= ctx->closures_.end(); ++it)
		{
			const Ads_Asset_Section_Closure *closure = it->second;
			if (!closure->is_slot_active(slot))
				continue;
			int guaranteed = closure->guaranteed(slot);
			// collect all effective hard guaranteed rules
			if (guaranteed > 0)
			{
				Ads_GUID_Pair_Set rules;
				ctx->collect_hard_guaranteed_through_rules(closure->network_id_, slot, rules);
				win_rules.insert(rules.begin(), rules.end());
			}

			const Ads_GUID_Pair_Set *rules = closure->opportunity_rules(slot);
			if (rules && !rules->empty())
			{
				if (used) used_rules.insert(rules->begin(), rules->end());
				else unused_rules.insert(rules->begin(), rules->end());
			}
		}
	}

	Ads_GUID_Pair_Set rules/*chance rule in empty slot and not in used slots*/;
	std::set_difference(unused_rules.begin(), unused_rules.end(), used_rules.begin(), used_rules.end(),
					std::insert_iterator<Ads_GUID_Pair_Set>(rules, rules.begin()));

	time_t now = ::time(NULL);
	int64_t dummy = 0;
	auto cui = std::shared_ptr<Ads_Selection_Context::Counter_Update_Info>(new Ads_Selection_Context::Counter_Update_Info);
	cui->timestamp_ = now;
	for (Ads_GUID_Pair_Set::const_iterator it = rules.begin(); it != rules.end(); ++it)
	{
		const Ads_MRM_Rule *ar = 0;
		if (ctx->repository_->find_access_rule(it->first, ar) >= 0 && ar &&
				!(ar->is_preference_guaranteed() || ar->volume_control_.unit_ == Volume_Control::SLOT_OPPORTUNITY))
		{
			int64_t magnifier = 1;
#if defined(ADS_ENABLE_FORECAST)
			if (Ads_Server_Config::instance()->enable_magnifier_)
				magnifier = ctx->request_->magnifier();
			cui->opportunity_rules_.insert(*it);
#else
			ctx->counter_service_->update_counter(0, ar, it->second, Counter_Spec::COUNTER_MRM_RULE_OPPORTUNITY, magnifier, now, dummy);
#endif
			if (!ar->is_old_application_rule() && /* ar->volume_control_.priority_ == Volume_Control::HARD_GUARANTEED && */
					win_rules.find(*it) != win_rules.end())
			{
#if defined(ADS_ENABLE_FORECAST)
				cui->rules_.push_back(*it);
#else
				ctx->counter_service_->update_counter(0, ar, it->second, Counter_Spec::COUNTER_MRM_RULE_DELIVERY, magnifier, now, dummy);
#endif
			}
		}
	}
#if defined(ADS_ENABLE_FORECAST)
	if(!cui->opportunity_rules_.empty())
		cui->to_string(ctx->callback_counters_for_wasted_inventory_);
#endif
	return 1;
}

void
Ads_Rules_Collection::append_win_inbound_rule(const Ads_MRM_Rule& outbound_rule, Ads_GUID reseller_network_id)
{
	// Ads_GUID inbound_rule_id = ads::entity::id(ADS_ENTITY_TYPE_INBOUND_MRM_RULE);
	const auto* inbound_rule = outbound_rule.get_inbound_rule(reseller_network_id);
	if (inbound_rule)
	{
		ADS_DEBUG((LP_TRACE, "adding inbound rule %s for reseller %s\n", ADS_ENTITY_ID_CSTR(inbound_rule->id_), ADS_ENTITY_ID_CSTR(reseller_network_id)));
		this->binary_log_inbound_win_rules_[ads::entity::restore_looped_id(reseller_network_id)].insert(inbound_rule->id_);
	}
}

bool
Ads_Rules_Collection::is_required_rule(const Ads_MRM_Rule& rule) const
{
	if (slot_ != nullptr && ref_ == nullptr)
	{
		return (is_prefetch_ && is_slot_rule(rule));
	}

	if (ref_ != nullptr && slot_ == nullptr)
	{
		return (!is_prefetch_ || !is_slot_rule(rule));
	}

	ADS_ASSERT(false);
	return false;
}

void
Ads_Rules_Collection::append_win_rule(const Ads_Selection_Context *ctx, Ads_GUID outbound_rule_id, Ads_GUID rule_ext_id, Ads_GUID reseller_network_id)
{
	const Ads_MRM_Rule *outbound_rule = nullptr;
	if (ctx->repository_->find_access_rule(outbound_rule_id, outbound_rule) >= 0 && outbound_rule)
	{
		if (is_required_rule(*outbound_rule))
		{
			ADS_DEBUG((LP_TRACE, "adding rule %s to delivery and opportunity counter\n", ADS_RULE_CSTR(outbound_rule)));
			binary_log_outbound_win_rules_[outbound_rule->network_id_].emplace(outbound_rule->application_rule_id_);
			binary_log_outbound_opportunity_rules_[outbound_rule->network_id_].emplace(outbound_rule->application_rule_id_, outbound_rule->is_old_application_rule() ? 0 : 1);

			append_win_inbound_rule(*outbound_rule, reseller_network_id);
			if (need_update_counter_)
			{
				counter_outbound_win_rules_.emplace_back(outbound_rule->id_, rule_ext_id);
				counter_outbound_opportunity_rules_.emplace(outbound_rule->id_, rule_ext_id);
			}
		}
	}
}

void
Ads_Rules_Collection::collect_candidate_win_rules(const Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate_Ref& ref)
{
	if (!ref.slot_ || !ref.candidate_)
		return;

	auto* slot = ref.slot_;
	const auto* candidate = ref.candidate_;

	const Ads_MRM_Rule *external_rule = candidate->external_rule(slot);
	if (external_rule != nullptr)
	{
		ADS_ASSERT(candidate->ad()->is_external_);
		auto reseller_network_id = candidate->ad()->external_network_id_;
		append_win_rule(ctx, external_rule->id_, candidate->external_rule_ext_id(slot), reseller_network_id);
	}


	const auto* path = candidate->access_path(slot).mrm_rule_path_;
	collect_path_win_rules(ctx, path);
}

void
Ads_Rules_Collection::collect_path_win_rules(const Ads_Selection_Context *ctx, const Ads_MRM_Rule_Path *path)
{
	if (!path)
		return;

	Ads_GUID_Pair_Set rule_ids;
	for (const auto& edge : path->edges_)
	{
		auto id = std::make_pair(edge.rule_id_, edge.rule_ext_id_);
		if (rule_ids.find(id) != rule_ids.end())
			continue;

		rule_ids.insert(id);
		append_win_rule(ctx, edge.rule_id_, edge.rule_ext_id_, edge.partner_id());
	}
}

void
Ads_Rules_Collection::collect_opportunity_rules(const Ads_Selection_Context *ctx, Ads_Slot_Base &slot)
{
	auto add_opportunity_rules = [&](const Ads_GUID_Pair_Set &rule_ids) {
		for (const auto &rule_id_pair : rule_ids)
		{
			const Ads_MRM_Rule *rule = nullptr;
			if (ctx->repository_->find_access_rule(rule_id_pair.first, rule) >= 0 && rule != nullptr)
			{
				if (this->is_required_rule(*rule))
				{
					ADS_DEBUG((LP_TRACE, "adding opportunity rules %s on slot %s-%d to opportunity counter\n", ADS_RULE_CSTR(rule), slot.custom_id_.c_str(), slot.index()));
					this->binary_log_outbound_opportunity_rules_[rule->network_id_].emplace(rule->application_rule_id_, rule->is_old_application_rule() ? 0 : 1);
					if (this->need_update_counter_)
						counter_outbound_opportunity_rules_.emplace(rule->id_, rule_id_pair.second);
				}
			}
		}
	};

	for (const auto &closure_pair : ctx->closures_)
	{
		const auto *closure = closure_pair.second;
		auto sit = closure->slot_references_.find(&slot);
		if (sit == closure->slot_references_.end())
			continue;

		const Ads_Asset_Section_Closure::Slot_Ref &slot_ref = sit->second;
		if (!slot_ref.resellable_ && !slot_ref.guaranteed_)
			continue;

		add_opportunity_rules(slot_ref.application_opportunity_rules_);
	}

	for (const auto &execution_node : ctx->mkpl_executor_->execution_nodes())
	{
		add_opportunity_rules(execution_node.opportunity_external_application_rules(&slot));
	}
}

void
Ads_Rules_Collection::populate_counter_rules(Ads_Video_Slot::Counter_Rules &counter_rules) const
{
	for (const auto &network_win_rules : binary_log_outbound_win_rules_)
	{
		counter_rules.outbound_rules_[network_win_rules.first].win_rules_.insert(network_win_rules.second.begin(), network_win_rules.second.end());
	}
	for (const auto &network_opportunity_rules : binary_log_outbound_opportunity_rules_)
	{
		for (const auto &rule_id_pair : network_opportunity_rules.second)
		{
			if (rule_id_pair.second == 1)
				counter_rules.need_logging_ = true;

			ADS_DEBUG((LP_TRACE, "adding impression opportunity rule %s\n", ADS_ENTITY_ID_CSTR(rule_id_pair.first)));
			counter_rules.outbound_rules_[network_opportunity_rules.first].opportunity_rules_.insert(rule_id_pair.first);
		}
	}

	for (const auto &network_win_inbound_rules : binary_log_inbound_win_rules_)
	{
		counter_rules.inbound_rules_[network_win_inbound_rules.first].win_rules_.insert(network_win_inbound_rules.second.begin(), network_win_inbound_rules.second.end());
	}
}

bool
Ads_Rules_Collection::is_slot_rule(const Ads_MRM_Rule& rule) const
{
	return (rule.is_preference_guaranteed() || rule.volume_control_.unit_ == Volume_Control::SLOT_OPPORTUNITY);
}

void
Ads_Rules_Collection::collect_slot_rules(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate_Ref_List& candidate_refs)
{
	if (slot_ == nullptr)
		return;

	for (const Ads_Advertisement_Candidate_Ref *ref : candidate_refs)
	{
		const Ads_Advertisement_Candidate *candidate = ref->candidate_;

		ADS_ASSERT(candidate);
		need_update_counter_ = ((candidate->flags_ & Ads_Advertisement_Candidate::FLAG_AF_NOT_UPDATE_RULE_COUNTER) == 0);
		collect_candidate_win_rules(ctx, *ref);
	}

	need_update_counter_ = true; // always update rules for phase 4 win rules & phase 2 opp rules
	const Ads_Advertisement_Candidate *lowest_candidate = find_slot_lowest_candidate(ctx, *slot_, candidate_refs);
	std::map<Ads_Slot_Base *, Ads_MRM_Rule_Path_Map>::const_iterator sit = ctx->access_paths().find(slot_);
	if (sit != ctx->access_paths().end())
	{
		const Ads_MRM_Rule_Path_Map &paths = sit->second;
		for (const auto &path_pair : paths)
		{
			const Ads_MRM_Rule_Path *path = path_pair.second;
			ADS_ASSERT(path);

			if (path->has_reactivated_rule_) continue;

			if (!path->has_higher_priority_than_candidate(ctx, slot_, lowest_candidate))
			{
				ADS_DEBUG((LP_INFO, "access path:\n\t%s\n failed to compete with candidate(%s)\n",
						   path->path_string().c_str(),
						   lowest_candidate ? CANDIDATE_AD_LPID_CSTR(lowest_candidate) : ""));
				continue;
			}

			collect_path_win_rules(ctx, path);
		}
	}

	collect_opportunity_rules(ctx, *slot_);
	return;
}

void
Ads_Rules_Collection::collect_candidate_rules(Ads_Selection_Context *ctx)
{
	if (!ref_ || !ref_->candidate_ || !ref_->slot_)
		return;

	need_update_counter_ = ((ref_->candidate_->flags_ & Ads_Advertisement_Candidate::FLAG_AF_NOT_UPDATE_RULE_COUNTER) == 0);

	///XXX: count in external rule id
	collect_candidate_win_rules(ctx, *ref_);
	collect_opportunity_rules(ctx, *ref_->slot_);

	return;
}
const Ads_Advertisement_Candidate*
Ads_Rules_Collection::find_slot_lowest_candidate(Ads_Selection_Context *ctx, const Ads_Video_Slot &slot, const Ads_Advertisement_Candidate_Ref_List& candidate_refs)
{
	const Ads_Advertisement_Candidate *lowest_candidate = nullptr;
	bool slot_is_fully_filled = !(slot.has_available_space(MAX_ACCEPTABLE_WASTED_DURATION_OF_SLOT));

	const Ads_Network *cro_network = nullptr;
	const Ads_GUID cro_network_id = ctx->root_network_id();
	if (!ctx->repository_ || ctx->repository_->find_network(cro_network_id, cro_network) < 0 || !cro_network)
	{
		ADS_DEBUG((LP_ERROR, "couldn't find cro network %s", ads::entity::str(cro_network_id).c_str()));
	}

	bool only_count_deliverable_rule = (cro_network != nullptr && cro_network->only_count_deliverable_rule());

	auto ad_greater = [ctx](const Ads_Advertisement_Candidate *left, const Ads_Advertisement_Candidate *right, const Ads_Slot_Base &slot) -> bool
	{
		if (left == nullptr || left->is_scheduled_in_slot(&slot)) return true;
		if (right == nullptr || right->is_scheduled_in_slot(&slot)) return false;

		Ads_Advertisement_Candidate::greater candidate_greater(ctx, ads::MEDIA_VIDEO);
		return candidate_greater(left, right);
	};

	if (only_count_deliverable_rule && slot_is_fully_filled)
	{
		for (const Ads_Advertisement_Candidate_Ref *ref : candidate_refs)
		{
			const Ads_Advertisement_Candidate *candidate = ref->candidate_;

			if (candidate->is_bumper()) continue;

			if (lowest_candidate == nullptr || ad_greater(lowest_candidate, candidate, slot))
			{
				lowest_candidate = candidate;
			}
		}
		ADS_DEBUG((LP_INFO, "slot %s: lowest priority ad %s\n", slot.custom_id_.c_str(),
				   lowest_candidate ? CANDIDATE_AD_LPID_CSTR(lowest_candidate) : ""));
	}

	return lowest_candidate;
}

void
Ads_MKPL_Orders_Collection::collect_slot_orders(const Ads_Selection_Context *ctx, const Ads_Video_Slot& slot)
{
	if (ctx->mkpl_executor_ == nullptr)
		return;

	mkpl::Avails_Calculator avails_calculator;
	for (const auto &execution_node : ctx->mkpl_executor_->execution_nodes())
	{
		if (execution_node.inbound_order_ == nullptr)
			continue;
		const auto *slot_ref = execution_node.slot_ref(&slot);
		if (slot_ref == nullptr)
			continue;

		int unfilled_avails = 0;
		int true_avails = avails_calculator.calculate_true_avails(*slot_ref, mkpl::DEFAULT_DURATION_30S, execution_node.id_, unfilled_avails);

		if (true_avails <= 0)
			continue;

		Ads_GUID order_id = execution_node.inbound_order_->id();
		counter_mkpl_orders_.emplace_back();
		auto &counter_order = counter_mkpl_orders_.back();

		counter_order.order_id_ = order_id;
		counter_order.true_avails_ = true_avails;
	}
}

void
Ads_MKPL_Orders_Collection::collect_candidate_orders(const Ads_Advertisement_Candidate_Ref& ref)
{
	const auto& access_path = ref.candidate_->access_path(ref.slot_);
	if (access_path.mkpl_path_ != nullptr)
	{
		counter_mkpl_orders_.reserve(access_path.mkpl_path_->size() - 1);
		access_path.mkpl_path_->each_order([&](const mkpl::Order_Candidate& order) {
			counter_mkpl_orders_.emplace_back();
			auto &counter_order = counter_mkpl_orders_.back();

			counter_order.order_id_ = order.id();
			counter_order.allocation_criteria_ids_ = order.effective_allocation_criteria_ids_;
		});
	}
}

int
Ads_Selector::update_slot_callback_counters(Ads_Selection_Context *ctx, bool is_prefetch, int magnifier)
{
	std::map<const Ads_Slot_Base*, Ads_Advertisement_Candidate_Ref_List> slot_candidate_map;

	for (Ads_Advertisement_Candidate_Ref *ref : ctx->delivered_candidates_)
	{
		Ads_Slot_Base *slot = ref->slot_;
		if (slot != nullptr)
		{
			slot_candidate_map[slot].push_back(ref);
		}
	}

	//store all or none rules counters in slot impression callabck counter record
	for (Ads_Slot_Base *slot : ctx->video_slots_)
	{
		Ads_Video_Slot *vslot = static_cast<Ads_Video_Slot*>(slot);
		if (vslot == nullptr)
			continue;

		Ads_MKPL_Orders_Collection slot_mkpl_orders(ctx, *vslot);
		Ads_Rules_Collection slot_rules(ctx, *vslot, slot_candidate_map[slot], is_prefetch);

		auto& counter_mkpl_orders = slot_mkpl_orders.mkpl_orders();
		auto& counter_win_rules = slot_rules.get_counter_outbound_win_rules();
		auto& counter_opportunity_rules = slot_rules.get_counter_outbound_opportunity_rules();

		if (is_prefetch)
		{
			if (!counter_win_rules.empty() ||
			    !counter_opportunity_rules.empty() ||
			    !counter_mkpl_orders.empty())
			{
				Ads_GUID_Pair_Set counter_win_rules_set(std::begin(counter_win_rules), std::end(counter_win_rules)); //avoid break regression
				Counter_Update_Info cui;
				cui.magnifier_ = magnifier;
				cui.rules_.assign(counter_win_rules_set.begin(), counter_win_rules_set.end());
				cui.opportunity_rules_.swap(counter_opportunity_rules);
				cui.mkpl_orders_.swap(counter_mkpl_orders);
				cui.timestamp_ = ::time(nullptr);

				if (ctx->supports_slot_callback())
					cui.to_string(vslot->callback_counters_);
				else
				{
					//TODO: refactor, move FW_DEBUG_INTERNAL_CALL to should_update_counter.
					if (ctx->request_->p(FW_DEBUG_INTERNAL_CALL) != "1")
					{
	#if defined(ADS_ENABLE_FORECAST)
						cui.to_string(vslot->callback_counters_);
	#else
						const Ads_Advertisement *dummy_ads[2] = {nullptr, nullptr};
						ctx->counter_service_->update_counters(ctx->repository_, dummy_ads, cui.rules_, cui.opportunity_rules_, cui.magnifier_);

						for (const auto &order : cui.mkpl_orders_)
						{
							ctx->counter_service_->update_mkpl_order_true_avails(order.order_id_, order.true_avails_);
						}
	#endif
					}
				}
			}
		}
		else
		{
			for (const auto &order : counter_mkpl_orders)
			{
				ctx->counter_service_->update_mkpl_order_true_avails(order.order_id_, order.true_avails_);
			}
		}
	}
	return 0;
}

int
Ads_Selector::update_candidate_counter_callback(Ads_Selection_Context *ctx, bool is_prefetch, int magnifier)
{
	for (Ads_Advertisement_Candidate_Ref *ref : ctx->delivered_candidates_)
	{
		const Ads_Advertisement_Candidate *candidate = ref->candidate_;

		bool prefetch = is_prefetch;

		if ((ref->flags_ & Ads_Advertisement_Candidate_Ref::FLAG_PLACEHOLDER_AD) > 0)
			continue;

		ADS_ASSERT(candidate);

		Ads_Slot_Base *slot = ref->slot_;
		const Ads_Creative_Rendition *acreative = ref->creative_;

		ADS_ASSERT(candidate->ad_);

		if (candidate->ad_->is_tracking() && !ctx->request_->rep()->capabilities_.support_null_creative_)
			prefetch = false;

		Counter_Update_Info cui;
		cui.magnifier_ = magnifier;
		cui.timestamp_ = ::time(NULL);
		if (acreative)
		{
			cui.ad_id_ = candidate->ad_->id_;
			ADS_ASSERT(acreative);
			cui.creative_id_ = acreative->id_;
		}

		Ads_GUID_Pair_List counter_win_rules;
		Ads_GUID_Pair_Set counter_opportunity_rules;

		std::vector<Counter_Update_Info::MKPL_Order> counter_mkpl_orders;

		const auto& access_path = candidate->access_path(slot);
		if (access_path.mkpl_path_ != nullptr)
		{
			Ads_MKPL_Orders_Collection candidate_orders(*ref);
			counter_mkpl_orders.swap(candidate_orders.mkpl_orders());
		}

		Ads_Rules_Collection candidate_rules(ctx, *ref, prefetch);
		counter_win_rules.swap(candidate_rules.get_counter_outbound_win_rules());
		counter_opportunity_rules.swap(candidate_rules.get_counter_outbound_opportunity_rules());

		// update counters
		if (!prefetch)
		{
			ADS_DEBUG((LP_TRACE, "non prefetch: add rules to ad %s\n", ADS_ENTITY_ID_CSTR(candidate->ad_->id_)));
			const Ads_Advertisement *budget_ad = 0;
			if (!candidate->ad_->budget_exempt_ && ads::entity::is_valid_id(candidate->ad_->budget_control_id_))
				ctx->repository_->find_advertisement(candidate->ad_->budget_control_id_, budget_ad);

#if defined(ADS_ENABLE_FORECAST)
			cui.rules_.swap(counter_win_rules);
			cui.opportunity_rules_.swap(counter_opportunity_rules);
			cui.mkpl_orders_.swap(counter_mkpl_orders);
			cui.to_string(ref->callback_counters_);
#else
			{
				const Ads_Advertisement *ads[2] = {candidate->ad_, budget_ad};
				ctx->counter_service_->update_counters(ctx->repository_, ads, counter_win_rules,
						counter_opportunity_rules, magnifier);
				for (const auto &order : counter_mkpl_orders)
				{
					ctx->counter_service_->update_mkpl_order_delivery(order.order_id_, magnifier);
					for (auto criteria_id : order.allocation_criteria_ids_)
					{
						ctx->counter_service_->update_mkpl_order_delivery_on_allocation_criteria(order.order_id_, criteria_id, magnifier);
					}
				}
			}
#endif
		}
		else
		{
			ADS_DEBUG((LP_TRACE, "add rules to ad %s\n", ADS_ENTITY_ID_CSTR(candidate->ad_->id_)));
			cui.rules_.swap(counter_win_rules);
			cui.opportunity_rules_.swap(counter_opportunity_rules);
			cui.mkpl_orders_.swap(counter_mkpl_orders);
			cui.to_string(ref->callback_counters_);
		}
	}

	return 0;
}

int
Ads_Selector::update_rules_and_counters(Ads_Selection_Context *ctx, bool deferred)
{
	int magnifier = 1;
	if (ctx != nullptr && ctx->is_1_to_n_request())
	{
		int req_multiplier = ctx->get_1_to_n_device_multiplier();
		magnifier = req_multiplier > 0 ? req_multiplier : 1;
	}
	else if (Ads_Server_Config::instance()->enable_magnifier_)
	{
		magnifier = ctx->request_->magnifier();
	}

	// NOTE: use is_prefetch while not deferred below has a long history since 2010 085a7d1ead from jack
	bool is_prefetch = deferred;
	update_slot_callback_counters(ctx, is_prefetch, magnifier);
	update_candidate_counter_callback(ctx, is_prefetch, magnifier);

	return 0;
}


int
Ads_Selector::extract_targeted_terms(const Ads_Selection_Context *ctx, const Ads_GUID_Set &terms_pool, const Ads_Term_Criteria *criteria, Ads_GUID_Set& terms, bool recursive)
{
//	const Ads_Term_Criteria *criteria = candidate->ad_->targeting_criteria_;
	ADS_ASSERT(criteria);
	if (!criteria) return 0;

	const Ads_Term_Criteria_Map &criterias = (criteria->delta_ ? ctx->delta_repository_->criterias_ : ctx->repository_->criterias_);
	std::queue<const Ads_Term_Criteria*> pending_criterias;
	ads::hash_set<const Ads_Term_Criteria*> processed_criterias;

	pending_criterias.push(criteria);
	while (!pending_criterias.empty())
	{
		const Ads_Term_Criteria *current = pending_criterias.front();
		pending_criterias.pop();

		if (processed_criterias.find(current) != processed_criterias.end())
			continue;
		else
			processed_criterias.insert(current);

		if (current->negative_) continue;

		for (const auto *child : current->children_)
		{
			if (processed_criterias.find(child) == processed_criterias.end())
				pending_criterias.push(child);
		}

		for (Ads_GUID_RVector::const_iterator it = current->terms_.begin(); it != current->terms_.end(); ++it)
		{
			Ads_GUID term_id = *it;
			if (terms_pool.find(term_id) != terms_pool.end())
				terms.insert(term_id);
		}
	}

	if (recursive)
	{
		Ads_GUID_Set aterms = terms;
		for (Ads_GUID_Set::const_iterator it = aterms.begin(); it != aterms.end(); ++it)
		{
			Ads_GUID term_id = *it;
			if (ads::term::type(term_id) == Ads_Term::AUDIENCE_ITEM)
			{
				Ads_GUID audience_item_id = ads::entity::make_id(ADS_ENTITY_TYPE_AUDIENCE, ads::entity::id(term_id));
				const Ads_Audience_Item *audience_item = 0;
				if (ctx->repository_ && ctx->repository_->find_audience_item(audience_item_id, audience_item) >= 0 && audience_item)
				{
					this->extract_targeted_terms(ctx, terms_pool, audience_item->targeting_criteria_, terms, false);
				}
			}
			else if (ads::term::type(term_id) == Ads_Term::CONTENT_PACKAGE)
			{
				Ads_GUID content_package_id = ads::entity::make_id(ADS_ENTITY_TYPE_CONTENT_PACKAGE, ads::entity::id(term_id));
				const Ads_Content_Package *content_package = 0;
				if (ctx->repository_ && ctx->repository_->find_content_package(content_package_id, content_package) >= 0 && content_package)
				{
					this->extract_targeted_terms(ctx, terms_pool, content_package->targeting_criteria_, terms, false);
				}
			}
		}
	}

	return 0;
}

int
Ads_Selector::extract_xdevice_policy_targeted_terms(const Ads_Selection_Context &ctx,
                                                    const Ads_Asset_Section_Closure &closure,
                                                    const Ads_GUID vendor_id,
                                                    const Ads_String policy_string,
                                                    const Ads_Term_Criteria &id_graph_criteria,
                                                    Ads_GUID_Set& terms)
{
	Ads_GUID_Set terms_pool(closure.terms_);
	const auto &audience_info = closure.audience_info();
	const auto vendor_it = audience_info.policy_terms_.find(vendor_id);
	if (vendor_it != audience_info.policy_terms_.end())
	{
		auto *vendor = vendor_it->second.get();
		if (vendor != nullptr)
		{
			const Ads_GUID_Set *policy_terms = vendor->find_policy_terms(policy_string);
			if (policy_terms != nullptr)
				terms_pool.insert(policy_terms->begin(), policy_terms->end());
		}
	}
	this->extract_targeted_terms(&ctx, terms_pool, &id_graph_criteria, terms, true);
	return 0;
}

void
Ads_Selector::terms_to_string(const Ads_GUID_Set& terms, Ads_String& s)
{
	for (Ads_GUID_Set::const_iterator it = terms.begin(); it != terms.end(); ++it)
	{
		Ads_GUID term_id = *it;
		s += ads::i64_to_str(ads::term::type(term_id)) + "~" + ads::entity::str(term_id);
		s += "`";
	}
}

double
Ads_Selector::calculate_mrm_rule_boost_factor(const Ads_Selection_Context *ctx, Ads_GUID rule_id, Ads_GUID rule_ext_id, double& fill_rate)
{
	ADS_ASSERT(ctx && ctx->repository_);
	const Ads_Repository *repo = ctx->repository_;

	const Ads_MRM_Rule *rule = 0;
	if (repo->find_access_rule(rule_id, rule) < 0 || !rule)
	{
		ADS_DEBUG((LP_ERROR, "rule %s not found\n", ADS_RULE_ID_CSTR(rule_id)));
		return 1.0;
	}

	double bias = 1.0;
	if (rule->priority_ == Ads_MRM_Rule::SOFT_GUARANTEED)
		bias = repo->system_config().bias_soft_guaranteed_rule_;

	const auto &fill_rate_info = ctx->mrm_rule_fill_rate(*rule, rule_ext_id);
	fill_rate = fill_rate_info.fill_rate_;

	// FW-8348 deal oc percent rule boost factor always 1
	if (rule->deal_ != nullptr && rule->volume_control_.type_ == Volume_Control::PERCENT_PERIOD
		&& (rule->deal_->type_ == Ads_Deal::DEAL || rule->deal_->type_ == Ads_Deal::BACKFILL))
	{
		return bias;
	}

	const Ads_Control_Curve_Group *curve = ctx->repository_->system_config().boost_rule_;
	double boost = (curve != nullptr ? curve->apply(fill_rate_info.time_, fill_rate) : 1.0);

	bias *= boost;
	return bias;
}

inline long day_of_month(long month)
{
	static long days[13] = { 30, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
	return (month >= 1 && month <= 12) ? days[month] : days[0];
}

template<class Entity>
bool
Ads_Selector::calculate_fill_rate(const Ads_Selection_Context *ctx, const Entity &entity,
	time_t now_t, time_t effective_date, time_t end_date, const Ads_RString *tz_name,
	int64_t volume, int64_t delivery, selection::Fill_Rate &fill_rate,
	bool *met_ext_schedule, double *ext_fill_rate, int64_t *target_impression)
{
	fill_rate.time_ = 0.5;
	fill_rate.met_schedule_ = false;
	if (met_ext_schedule != nullptr)
		*met_ext_schedule = true;

	const auto &volume_control = entity.volume_control_;
	ADS_ASSERT(volume_control.type_ == Volume_Control::FIXED);

	if (tz_name != nullptr && !tz_name->empty())
	{
		effective_date = ctx->repository_->timezone_transform(effective_date, *tz_name);
		now_t = ctx->repository_->timezone_transform(now_t, *tz_name);
		end_date = ctx->repository_->timezone_transform(end_date, *tz_name);
	}
	struct tm start, now;
	::localtime_r(&effective_date, &start);
	::localtime_r(&now_t, &now);

	switch (volume_control.period_)
	{
		case Volume_Control::DAY:
			{
				int64_t start_seconds = (int64_t)(start.tm_hour*3600 + start.tm_min*60 + start.tm_sec);
				int64_t current_seconds = (int64_t)(now.tm_hour*3600 + now.tm_min*60 + now.tm_sec);
				if (now.tm_year == start.tm_year && now.tm_yday == start.tm_yday) //same day
				{
					volume =(volume * (24*3600-start_seconds))/(24*3600);
					fill_rate.time_ = (double)(current_seconds - start_seconds)/(24*3600 - start_seconds);
				}
				else
				{
					fill_rate.time_ = (double)current_seconds/(24*3600);
				}
				break;
			}

		case Volume_Control::WEEK:
			{
				int64_t current_seconds = (int64_t)(now.tm_wday*24*3600 + now.tm_hour*3600 + now.tm_min*60 + now.tm_sec);
				int64_t start_seconds = start.tm_wday*24*3600 + start.tm_hour*3600 + start.tm_min*60 + start.tm_sec;
				if ((now_t - effective_date) <= current_seconds)
				{
					volume = (volume * (7*24*3600-start_seconds))/(7*24*3600);
					fill_rate.time_ = (double)(current_seconds - start_seconds)/(7*24*3600 - start_seconds);
				}
				else
				{
					fill_rate.time_ = (double)current_seconds/(7*24*3600);
				}
				break;
			}

		case Volume_Control::MONTH:
			{
				long days = day_of_month(now.tm_mon + 1);
				int64_t start_seconds = (int64_t)((start.tm_mday-1)*24*3600 + start.tm_hour*3600 + start.tm_min*60 + start.tm_sec);
				int64_t current_seconds = (int64_t)((now.tm_mday-1)*24*3600 + now.tm_hour*3600 + now.tm_min*60 + now.tm_sec);
				if (now.tm_year == start.tm_year && now.tm_mon == start.tm_mon)
				{
					volume = (volume * (days*24*3600 - start_seconds))/(days*24*3600);
					fill_rate.time_ = (double)(current_seconds - start_seconds)/(days*24*3600 - start_seconds);
				}
				else
				{
					fill_rate.time_ = (double)current_seconds/(days*24*3600);
				}
				break;
			}
		case Volume_Control::LIFECYCLE:
			{
				if (effective_date > 0 && end_date > 0 && effective_date != end_date)
					fill_rate.time_ = 1.0 * (now_t - effective_date) / (end_date - effective_date);
				break;
			}

		default:
			break;
	}

	if (volume <= 0)
	{
		fill_rate.met_schedule_ = true;
		fill_rate.fill_rate_ = 1.0;
		return false;
	}
	double progress = (delivery  * 1.0 / volume);
	ADS_DEBUG((LP_TRACE, "%s delivered:%d, budget:%d, progress:%lf, time:%lf\n", ADS_ORDER_OR_RULE_CSTR(entity.id_), delivery, volume, progress, fill_rate.time_));
	double target = 1.0;
	switch (volume_control.pace_)
	{
		case Volume_Control::AS_FAST_AS_POSSIBLE:
		{
			fill_rate.time_ = 0.5;
			if (progress >= 1.0)
				fill_rate.met_schedule_ = true;
			fill_rate.fill_rate_ = progress;
			if (target_impression != nullptr)
				*target_impression = volume;
			return true;
		}
		case Volume_Control::CUSTOM:
		{
			if (volume_control.period_ != Volume_Control::LIFECYCLE ||
			    !get_custom_pacing_target(volume_control.custom_pacing_polyline_, fill_rate.time_, target))
			{
				target = fill_rate.normalized_target();
				ADS_DEBUG((LP_ERROR, "Custom pacing is not applicable for %s, fallback to EVEN pacing\n",
				           ADS_ORDER_OR_RULE_CSTR(entity.id_)));
			}
			break;
		}
		case Volume_Control::EVEN:
		{
			target = fill_rate.normalized_target();
			break;
		}
	}

	if (target_impression != nullptr)
		*target_impression = volume * target;

	if (volume_control.period_ == Volume_Control::LIFECYCLE &&
	    (effective_date == 0 || end_date == 0 || effective_date == end_date))
	{
		fill_rate.met_schedule_ = true;
		ADS_DEBUG((LP_TRACE, "%s using lifecycle period must have valid effective date and end date.\n",
		           ADS_ORDER_OR_RULE_CSTR(entity.id_)));
		fill_rate.fill_rate_ = 1.0;
		return false;
	}

	fill_rate.fill_rate_ = progress / target;
	ADS_DEBUG((LP_TRACE, "%s fill_rate:%f\n", ADS_ORDER_OR_RULE_CSTR(entity.id_), fill_rate.fill_rate_));
	if (fill_rate.fill_rate_ >= 1.0)
	{
		fill_rate.met_schedule_ = true;
		ADS_DEBUG((LP_TRACE, "%s met schedule (%f >= 1.0).\n", ADS_ORDER_OR_RULE_CSTR(entity.id_), fill_rate.fill_rate_));

		if (met_ext_schedule != nullptr || ext_fill_rate != nullptr)
			calculate_ext_fill_rate(ctx, ADS_ORDER_OR_RULE_STR(entity.id_), fill_rate, progress, volume_control.ext_delivery_curve_id_, met_ext_schedule, ext_fill_rate);
	}
	return true;
}

template bool
Ads_Selector::calculate_fill_rate(const Ads_Selection_Context *ctx, const Ads_MKPL_Order &entity,
	time_t now_t, time_t effective_date, time_t end_date, const Ads_RString *tz_name,
	int64_t volume, int64_t delivery, selection::Fill_Rate &fill_rate,
	bool *met_ext_schedule, double *ext_fill_rate, int64_t *target_impression);

template bool
Ads_Selector::calculate_fill_rate(const Ads_Selection_Context *ctx, const Ads_Deal &entity,
	time_t now_t, time_t effective_date, time_t end_date, const Ads_RString *tz_name,
	int64_t volume, int64_t delivery, selection::Fill_Rate &fill_rate,
	bool *met_ext_schedule, double *ext_fill_rate, int64_t *target_impression);

bool
Ads_Selector::get_custom_pacing_target(const Ads_Custom_Pacing_Polyline &custom_pacing_polyline, double time, double& target)
{
	if (!custom_pacing_polyline.is_valid())
	{
		ADS_ERROR_RETURN((LP_ERROR,
		                  "Custom pacing is not applicable\n"), false);
	}
	target = custom_pacing_polyline.get_delivery_target(time);
	return true;
}

void
Ads_Selector::calculate_mrm_rule_fill_rate(const Ads_Selection_Context *ctx, const Ads_MRM_Rule &rule, Ads_GUID rule_ext_id, selection::Fill_Rate &fill_rate,
						bool *met_ext_schedule, double *ext_fill_rate, const std::map<Ads_GUID_Pair, std::pair<int64_t, int64_t> > *ext_rule_counters)
{
	ADS_ASSERT(ctx && ctx->repository_);

	double progress = 0.0;
	double target = 1.0;
	fill_rate.time_ = 0.5; // magic number (middle stage)
	fill_rate.met_schedule_ = false;
	if (met_ext_schedule != nullptr)
		*met_ext_schedule = true;

	const auto *volume_control = &rule.volume_control_;
	if (rule_ext_id > 0)
	{
		const Ads_MRM_Rule::Sub_Rule *srule = rule.sub_rule(rule_ext_id);
		if (srule)
			volume_control = &srule->volume_control_;
	}
	if (volume_control->type_ == Volume_Control::PERCENT_PERIOD)
	{
		progress = this->calculate_mrm_rule_delivery_progress(ctx, &rule, rule_ext_id, ext_rule_counters);
		if (progress >= 1.0)
		{
			fill_rate.met_schedule_ = true;
		}
		fill_rate.fill_rate_ = progress;
		return;
	}
	else
	{
		time_t t = ctx->time();
		ADS_ASSERT(t >= rule.effective_date_);

		int64_t snapshot_delivered = rule.snapshot_delivered();
		time_t effective_date = rule.snapshot_date();
		effective_date = std::max(effective_date, rule.effective_date_);
		time_t end_date = rule.end_date_;

		const Ads_RString *tz_name = nullptr;
#if !defined(ADS_ENABLE_FORECAST)
		const Ads_Network *network = nullptr;
		if(ads::entity::is_valid_id(rule.network_id_) && ctx->repository_->find_network(rule.network_id_, network) != -1 && network->allow_reset_by_timezone_from(t))
		{
			tz_name = &rule.timezone_name_;
		}
#endif
		if (rule.is_old_application_rule())
		{
			if (tz_name != nullptr)
			{
				effective_date = ctx->repository_->timezone_transform(effective_date, *tz_name);
				t = ctx->repository_->timezone_transform(t, *tz_name);
				end_date = ctx->repository_->timezone_transform(end_date, *tz_name);
			}
			struct tm start, now;
			::localtime_r(&effective_date, &start);
			::localtime_r(&t, &now);

			progress = this->calculate_mrm_rule_delivery_progress(ctx, &rule, rule_ext_id);
			switch (volume_control->period_)
			{
				case Volume_Control::DAY:
				{
						/// not handling
					break;
				}
				case Volume_Control::WEEK:
				{
					if (start.tm_wday > 0 && (now.tm_year == start.tm_year && now.tm_mon == start.tm_mon && now.tm_mday - start.tm_mday < 7 - start.tm_wday))
					{
						fill_rate.time_ = (double)(now.tm_wday - start.tm_wday + 1) / (7 - start.tm_wday);
					}
					else
					{
						fill_rate.time_ = (double)(now.tm_wday + 1) / 7;
					}

					target = fill_rate.normalized_target(); // Ads_Booster::instance()->schedule("ad.default", time);
					break;
				}
				case Volume_Control::MONTH:
				{
					long days = day_of_month(now.tm_mon + 1);
					if (start.tm_mday > 0 && (now.tm_year == start.tm_year && now.tm_mon == start.tm_mon))
					{
						fill_rate.time_ = (double)(now.tm_mday - start.tm_mday + 1) / (days - start.tm_mday + 1);
					}
					else
					{
						fill_rate.time_ = (double)(now.tm_mday) / days;
					}

					target = fill_rate.normalized_target(); // Ads_Booster::instance()->schedule("ad.default", time);
					break;
				}
				default:
					break;
			}

			if (progress >= 1.0)
				fill_rate.met_schedule_ = true;
			fill_rate.fill_rate_ = progress / target;
		}
		else
		{
			int64_t n_delivery = 0;
			time_t timestamp = 0;
			if (ctx->read_counter(rule.id_, rule_ext_id, Counter_Spec::COUNTER_MRM_RULE_DELIVERY, n_delivery, timestamp) < 0)
			{
				ADS_DEBUG((LP_ERROR, "read_counter of rule (FIXED) failed\n", ADS_RULE_CSTR(&rule)));
				fill_rate.met_schedule_ = true;
				fill_rate.fill_rate_ = 1.0;
				return;
			}

			int64_t volume = volume_control->budget_ - snapshot_delivered;
			n_delivery -= snapshot_delivered;

			calculate_fill_rate(ctx, rule, t, effective_date, end_date, tz_name, volume, n_delivery, fill_rate,
				met_ext_schedule, ext_fill_rate);
		}
	}
	return;
}

void
Ads_Selector::calculate_ext_fill_rate(const Ads_Selection_Context *ctx, const Ads_String &entity, selection::Fill_Rate &fill_rate, double progress, Ads_GUID ext_delivery_curve_id,
					bool *met_ext_schedule, double *ext_fill_rate)
{
	bool dummy_met_ext_schedule;
	if (met_ext_schedule == nullptr)
		met_ext_schedule = &dummy_met_ext_schedule;
	*met_ext_schedule = true;

	double dummy_ext_fill_rate;
	if (ext_fill_rate == nullptr)
		ext_fill_rate = &dummy_ext_fill_rate;

	const Ads_Control_Curve *ext_curve = nullptr;
	if (ads::entity::is_valid_id(ext_delivery_curve_id))
		ctx->repository_->find_curve(ext_delivery_curve_id, ext_curve);
	if (ext_curve != nullptr)
	{
		double ext_target = ext_curve->apply(fill_rate.time_);
		*ext_fill_rate = std::min(progress / ext_target, 2.0);
		ADS_DEBUG((LP_TRACE, "%s ext_fill_rate:%f\n", entity.c_str(), *ext_fill_rate));
		if (*ext_fill_rate >= 1.0)
			ADS_DEBUG((LP_TRACE, "%s met ext schedule (%f >= 1.0).\n", entity.c_str(), *ext_fill_rate));
		else
			*met_ext_schedule = false;
	}
}

double
Ads_Selector::calculate_mrm_rule_delivery_progress(const Ads_Selection_Context *ctx, const Ads_MRM_Rule *rule, Ads_GUID rule_ext_id, const std::map<Ads_GUID_Pair, std::pair<int64_t, int64_t> > *ext_rule_counters)
{
	ADS_ASSERT(ctx && ctx->repository_);
	int64_t n_delivery = 0;
	const auto *volume_control = &rule->volume_control_;
	if (rule_ext_id > 0)
	{
		const Ads_MRM_Rule::Sub_Rule *srule = rule->sub_rule(rule_ext_id);
		if (srule)
			volume_control = &srule->volume_control_;
	}

	if (volume_control->type_ == Volume_Control::PERCENT_PERIOD)
	{
		time_t timestamp = 0;
		int64_t n_opportunity = 0;
		if (ctx->counter_service_->read_counter(rule->id_, rule_ext_id, Counter_Spec::COUNTER_MRM_RULE_DELIVERY, n_delivery, timestamp) < 0 ||
		        ctx->counter_service_->read_counter(rule->id_, rule_ext_id, Counter_Spec::COUNTER_MRM_RULE_OPPORTUNITY, n_opportunity, timestamp) < 0)
		{
			ADS_DEBUG((LP_ERROR, "read_counter of rule (PERCENTAGE) failed\n", ADS_RULE_CSTR(rule)));
			return (1.0);
		}

		if (ext_rule_counters)
		{
			std::map<Ads_GUID_Pair, std::pair<int64_t, int64_t> >::const_iterator it = ext_rule_counters->find(std::make_pair(rule->id_, rule_ext_id));
			if (it != ext_rule_counters->end())
			{
				n_delivery += it->second.first;
				n_opportunity += it->second.second;
			}
		}

		if (volume_control->budget_ == 0)
		{
			return (1.0);
		}

		if (n_delivery == 0 && n_opportunity == 0)
		{
			return (0.0);
		}

		if (n_delivery < 0 || n_opportunity <= 0)
		{
			ADS_DEBUG((LP_ERROR, "volume control restriction of rule %s (PERCENTAGE) failed\n", ADS_RULE_CSTR(rule)));
			return (1.0);
		}

		///XXX: 100% rule workaround
		//if (volume_control_budget == ADS_TO_MAGNIFIED_PERCENTAGE(1.0) && rule->is_hard_guaranteed())
		if (volume_control->budget_ == ADS_TO_MAGNIFIED_PERCENTAGE(1.0))
			return (std::min(0.999, n_delivery  * 1.0 / n_opportunity));

		return (n_delivery  * 1.0 / n_opportunity / ADS_TO_REAL_PERCENTAGE(volume_control->budget_));
	}
	else if (volume_control->type_ == Volume_Control::FIXED)
	{
		time_t timestamp = 0;
		if (ctx->counter_service_->read_counter(rule->id_, rule_ext_id, Counter_Spec::COUNTER_MRM_RULE_DELIVERY, n_delivery, timestamp) < 0)
		{
			ADS_DEBUG((LP_ERROR, "read_counter of rule (FIXED) failed\n", ADS_RULE_CSTR(rule)));
			return (1.0);
		}

		int64_t volume = volume_control->budget_;

		time_t t = ctx->time();
		ADS_ASSERT(t >= rule->effective_date_);
		time_t effective_date = rule->effective_date_;

		struct tm start, now;
#if defined(ADS_ENABLE_FORECAST)
		::localtime_r(&effective_date, &start);
		::localtime_r(&t, &now);
#else
		const Ads_Network *network = nullptr;
		if(ads::entity::is_valid_id(rule->network_id_) && ctx->repository_->find_network(rule->network_id_, network) != -1 && network->allow_reset_by_timezone_from(t))
		{
			effective_date = rule->to_my_timezone(ctx->repository_, effective_date);
			t = rule->to_my_timezone(ctx->repository_, t);
		}
		::localtime_r(&effective_date, &start);
		::localtime_r(&t, &now);
#endif

		switch (volume_control->period_)
		{
		case Volume_Control::DAY:
		{
			/// not handling
			break;
		}

		case Volume_Control::WEEK:
		{
//			ACE_Date_Time start(ads::Time_Value(rule->effective_date_, 0));
//			ACE_Date_Time now(ads::Time_Value(t, 0));

			if (start.tm_wday > 0 && (now.tm_year == start.tm_year && now.tm_mon == start.tm_mon && now.tm_mday - start.tm_mday < 7 - start.tm_wday))
			{
				volume = volume * (7 - start.tm_wday) / 7;
			}

			break;
		}

		case Volume_Control::MONTH:
		{
//			ACE_Date_Time start(ads::Time_Value(rule->effective_date_, 0));
//			ACE_Date_Time now(ads::Time_Value(t, 0));

			long days = day_of_month(now.tm_mon + 1);

			if (start.tm_mday > 0 && (now.tm_year == start.tm_year && now.tm_mon == start.tm_mon))
			{
				volume = volume * (days - start.tm_mday + 1) / days;
			}

			break;
		}

		default:
			break;
		}

		if (volume <= 0)
		{
			return (1.0);
		}

		return (n_delivery  * 1.0 / volume);
	}

	return (1.0);
}

/**
 * Simply forward to calculate_advertisement_boost_factor accepting
 * Ads_Advertisement as parameter, since this function is also used by
 * Ads_Suggestor.cpp
 */
double
Ads_Selector::calculate_advertisement_boost_factor(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate_Lite *ad, double life_stage, time_t time, double fill_rate)
{
	auto end_date = ad->ad()->end_date_;

	const auto *deal = ad->get_deal();
	if (deal)
	{
		end_date = deal->rule_ ? deal->rule_->end_date_ : deal->mkpl_order_ ? deal->mkpl_order_->end_time_ : end_date;
	}

	return calculate_advertisement_boost_factor(ctx, &ad->operation_owner(), ad->ad(), end_date, time, life_stage, fill_rate, ctx->is_watched(*ad) ? ctx->get_watched_id(*ad) : ads::entity::invalid_id(0));
}

double
Ads_Selector::calculate_advertisement_boost_factor(Ads_Selection_Context *ctx, const selection::Advertisement_Owner *ad_owner, const Ads_Advertisement *ad, time_t end_date, time_t time, double life_stage, double fill_rate, Ads_GUID watched_id)
{
	ADS_ASSERT(ctx && ctx->repository_);

	double bias = 1.0;

	///prefer the compensate curve
	bool use = false;
	double bias_saved = bias;

	double compensate_weight = 0;
	Ads_GUID compensate_curve_id = -1;

	if (ads::entity::is_valid_id(ad_owner->ad_delivery_compensate_config_id_))
	{
		const Ads_Control_Curve_Group *cg = 0;
		if (ctx->repository_->find_curve_group(ad_owner->ad_delivery_compensate_config_id_, cg) >= 0 && cg)
		{
			if (end_date > 0)
			{
				double days_left = (end_date - time) / (24.0 * 60 * 60);
				if (days_left > 0 && days_left < cg->scale_)
				{
					compensate_weight = cg->apply(days_left, fill_rate);
					compensate_curve_id = cg->id_;

					bias *= compensate_weight;
					use = true;
				}
			}
		}
	}

	///and then the boost curve
	double boost_weight = 0;
	Ads_GUID boost_curve_id = -1;
	if (!use)
	{
		Ads_GUID boost_id = ad->delivery_boost_id_;
		const Ads_Control_Curve_Group *cg = 0;

		///FIXME: only enabled in O&O?
		if (!ads::entity::is_valid_id(boost_id)
			&& (ctx->is_hylda_suggestion() || ctx->root_network_id(ads::MEDIA_VIDEO) == ctx->root_network_id(ads::MEDIA_SITE_SECTION)))
				boost_id = ad_owner->ad_delivery_boost_config_id_;

		if (ads::entity::is_valid_id(boost_id))
			ctx->repository_->find_curve_group(boost_id, cg);
		if (!cg) cg = ctx->repository_->system_config().boost_ad_;

		if (cg)
		{
			boost_weight = cg->apply(life_stage, fill_rate);
			boost_curve_id = cg->id_;
			bias *= boost_weight;
		}
	}

	///historical avail boost
	double avail_weight = 0;
	int64_t max_avail = 0;
	Ads_GUID avail_curve_id = -1;
	if (ads::entity::is_valid_id(ad_owner->historical_avail_boost_config_id_))
	{
		const Ads_Control_Curve *curve = 0;
		if (ctx->repository_->find_curve(ad_owner->historical_avail_boost_config_id_, curve) >= 0 && curve)
		{
			std::map<Ads_GUID, int64_t, std::less<Ads_GUID>, ads::allocator<int64_t> >::const_iterator it = ctx->repository_->network_max_avail_.find(ad_owner->network_.id_);
			if (it != ctx->repository_->network_max_avail_.end())
			{
				max_avail = it->second;
				if (max_avail > 0)
				{
					double historical_avail_ratio = 0.5; //default value for no history ads
					if (ad->historical_avail_ >= 0)
						historical_avail_ratio = (double) ad->historical_avail_ / max_avail;
					ADS_DEBUG((LP_TRACE, "boosting ad %s, base on historical avail %s, max avail %s, historical avail ratio %.5f, boost factor %.5f.\n",
						ADS_ADVERTISEMENT_ID_CSTR(ad->id_), ads::i64_to_str(ad->historical_avail_).c_str(), ads::i64_to_str(max_avail).c_str(),
						historical_avail_ratio, curve->apply(historical_avail_ratio) ));
					avail_curve_id = curve->id_;
					avail_weight = curve->apply(historical_avail_ratio);
					bias *= avail_weight;
				}
			}
		}
	}

	if (ctx->is_watched(watched_id))
	{
		json::ObjectP p;
		p["bias_saved"] = json::Number(bias_saved);

//		if (ads::entity::is_valid_id(compensate_curve_id))
		{
			p["compensate_curve_id"] = json::Number(compensate_curve_id);
			p["compensate_weight"] = json::Number(compensate_weight);
		}

//		if (ads::entity::is_valid_id(boost_curve_id))
		{
			p["boost_curve_id"] = json::Number(boost_curve_id);
			p["boost_weight"] = json::Number(boost_weight);
		}

//		if (ads::entity::is_valid_id(avail_curve_id))
		{
			p["avail_curve_id"] = json::Number(avail_curve_id);
			p["avail_weight"] = json::Number(avail_weight);
			p["max_avail"] = json::Number(max_avail);
			p["historical_avail"] = json::Number(ad->historical_avail_);
		}

	//	p["prob_boost"] = json::Number(prob_boost);
		p["total_weight"] = json::Number(bias);

		ctx->log_watched_info(watched_id, "boost", p);
	}

	ADS_DEBUG((LP_TRACE, "All the Boost Factor for Ad:%s, compensate boost weight:%f, delivery boost weight:%f, avail boost weight:%f; Overall Boost Weight:%f\n", ADS_ENTITY_ID_CSTR(ad->id_),
				compensate_weight, boost_weight, avail_weight, bias));

	return bias;
}

int
Ads_Selector::calculate_cpx_compositional_billable_rate(const Ads_Advertisement *ad, double &compositional_billable_rate)
{
    Ads_Enhanced_Shared_Data_Client& counter_service = Ads_Enhanced_Shared_Data_Client::instance();
    if (!ad)
        return -1;
    compositional_billable_rate = 0.0;
    int64_t counter_participating_recorded = 0, counter_participating_denominator = 0;
    time_t timestamp = 0;
    counter_service.read_counter(ad->id_, Counter_Spec::EXT_ID_COMPOSITIONAL_RECORDED, Counter_Spec::COUNTER_ADVERTISEMENT_IMPRESSION, counter_participating_recorded, timestamp);
    counter_service.read_counter(ad->id_, Counter_Spec::EXT_ID_COMPOSITIONAL_MEASURED, Counter_Spec::COUNTER_ADVERTISEMENT_IMPRESSION, counter_participating_denominator, timestamp);
    if (counter_participating_denominator > 0 || ad->participating_denominator_events_ > 0)
    {
        double ratio_c = counter_participating_denominator > 0 ? double(counter_participating_recorded) / counter_participating_denominator : 0.0,
               ratio_p = ad->participating_denominator_events_ > 0 ? double(ad->participating_recorded_abstract_events_) / ad->participating_denominator_events_ : 0.0;
        if (counter_participating_denominator > ad->participating_denominator_events_)
        {
            compositional_billable_rate = ratio_c != 0.0 ? ratio_c : ratio_p;
        }
        else if (counter_participating_denominator == ad->participating_denominator_events_)
        {
            if (counter_participating_recorded >= ad->participating_recorded_abstract_events_)
                compositional_billable_rate = ratio_c;
            else
                compositional_billable_rate = ratio_p;
        }
        else
            compositional_billable_rate = ratio_p != 0.0 ? ratio_p : ratio_c;

        if (compositional_billable_rate > 1.0)
            compositional_billable_rate = 1.0;
    }
    return 0;
}

int
Ads_Selector::activate_selection(const Ads_Repository * repo, Ads_Request *&req, Ads_Response *res, Ads_Selection_Context *&ctx)
{
	res->user(ctx->request_->user());

	req = ctx->request_;
	//ADS_ASSERT(req->detached()); // failed assertion
	req->detached(false);
	res->detached(false);

	/// update fields
	ctx->response_ = res;
	ctx->repository_ = repo;
	ctx->active_ = true;
#if !defined(ADS_ENABLE_FORECAST)
	ctx->enable_deep_lookup_ = true;
#endif

	return 0;
}

int
Ads_Selector::create_selection(const Ads_String& script, const Ads_Repository *repo, Ads_Request *req, Ads_Response *res, ads::Error_List *errors, Ads_Selection_Context *&ctx)
{
	ADS_ASSERT(req && res);

	res->user(req->user());
	ctx = Ads_Selection_Context::create(repo, req, res);

	ctx->self_ = new std::shared_ptr<Ads_Selection_Context>(ctx);

	ADS_DEBUG((LP_TRACE, "selecting for asset:%s, section:%s, caller: %s\n", ADS_ASSET_ID_CSTR(req->asset_id()), ADS_ASSET_ID_CSTR(req->section_id()), req->rep()? req->rep()->visitor_.caller_.c_str(): ""));

	ADS_ASSERT(ctx);
	ctx->counter_service_ = &Ads_Enhanced_Shared_Data_Client::instance();
	ctx->user_ = req->user();
	ctx->menv_ = new Ads_Macro_Environment();

	ctx->transaction_id_ = req->transaction_id();
	ctx->scheduler_->set_transaction_id(ctx->transaction_id_);
	ctx->verbose_ = req->verbose();

	ctx->menv_->selection_ = ctx;
	ctx->menv_->repo_ = ctx->repository_;
	ctx->menv_->request_ = req;
	ctx->menv_->transaction_id_ = ctx->transaction_id_;
	ctx->menv_->user_id_ = req->id_repo_.user_id().id();
	ctx->menv_->device_id_ = req->id_repo_.device_id().id();

	if (repo->system_config().is_xss_attack_protection_enabled_for_network(req->network_id()))
	{
		ctx->menv_->is_xss_attack_protection_enabled_ = true;
	}

	ctx->script_ = script;
	ctx->enable_auto_callbacks_ = ctx->request_->rep()->capabilities_.support_auto_callbacks_;

	if (errors) ctx->errors_.swap(*errors);

	bool enable_tracer = false;
	if (Ads_Server_Config::instance()->enable_tracer_ && req->o().tracer_ == "ui_debug")
	{
		// client address
		const Ads_String & addr = req->client_addr();
		if (!addr.empty())
		{
			uint32_t i = inet_addr(addr.c_str());
			if (i != (uint32_t)-1)
			{
				if (repo->is_address_privileged(ntohl(i)))
					enable_tracer = true;
			}
		}

		// token
		const char *s = repo->system_config().tracer_token_.c_str(); // repo->system_variable("selector.tracer_token");
		if (s&& s[0])
		{
			if (req->o().token_ == s) enable_tracer = true;
		}
		else
		{
			if (Ads_Server_Config::instance()->enable_debug_)
				enable_tracer = true;
		}
	}

	if (enable_tracer)
	{
		ctx->flags_ |= Ads_Selection_Context::FLAG_ENABLE_TRACER;
	}

	/// profile and flags
	const Ads_String& span_style = ctx->request_->p(FW_SPAN_STYLE);
	if (!span_style.empty())
	{
		if (span_style == "b")
			ctx->profile_span_style_ = "display:block; vertical-align:top; margin:#{ad.creative.marginHeight}px auto; width:#{ad.creative.scaleWidth}px; height:#{ad.creative.scaleHeight}px;";
		else if (span_style == "h")
			ctx->profile_span_style_ = "display:inline-block; overflow:hidden; vertical-align:top; margin:#{ad.creative.marginHeight}px #{ad.creative.marginWidth}px #{ad.creative.marginHeight}px #{ad.creative.marginWidth}px;";
		else if (span_style == "m")
			ctx->profile_span_style_ = "display:inline-block; vertical-align:top; margin:0;";
	}

	// Valid system level external ad timeout setting
	if (repo->system_config().external_ad_timeout_ > 0)
	{
		ctx->external_ad_timeout_ = repo->system_config().external_ad_timeout_;
	}
	// Valid system level external creative timeout setting
	if (repo->system_config().external_creative_timeout_ > 0)
	{
		ctx->external_creative_timeout_ = repo->system_config().external_creative_timeout_;
	}
	// Valid system level external notification timeout setting
	if (repo->system_config().external_notification_timeout_ > 0)
	{
		ctx->external_notification_timeout_ = repo->system_config().external_notification_timeout_;
	}

	if (repo->system_config().max_openrtb_imp_count_per_auction_ > 0)
	{
		ctx->max_openrtb_imp_count_per_auction_ = repo->system_config().max_openrtb_imp_count_per_auction_;
	}

	if (repo->system_config().external_candidate_max_load_rounds_ > 0)
	{
		ctx->external_candidate_max_load_rounds_ = repo->system_config().external_candidate_max_load_rounds_;
	}
	//CPx concrete event list from player request flags
	if (!ctx->request_->rep()->key_values_.impr_concrete_event_items_.empty())
	{
		for (Ads_String_List::const_iterator cit_event_items=ctx->request_->rep()->key_values_.impr_concrete_event_items_.begin();
				cit_event_items != ctx->request_->rep()->key_values_.impr_concrete_event_items_.end(); ++cit_event_items)
		{
			Ads_String event_item = *cit_event_items;
			if (event_item.empty()) continue;

			Ads_GUID concrete_event_id = ads::str_to_entity_id(ADS_ENTITY_TYPE_CONCRETE_EVENT, event_item);
			auto cit_concrete_event = repo->concrete_events_.find(concrete_event_id);
			if (cit_concrete_event == repo->concrete_events_.end()) continue;

			ctx->player_concrete_events_["i"][CALLBACK_NAME_DEFAULT_IMPRESSION].insert(concrete_event_id);
		}
	}

	if (ctx->request_flags() & Ads_Request::Smart_Rep::VERBOSE_LOG_PERFORMANCE)
		ctx->flags_ |= Ads_Selection_Context::FLAG_TRACK_PERFORMANCE;

	for (Ads_String_List::const_iterator it = req->rep()->key_values_.ad_asset_stores_.begin();
		it != req->rep()->key_values_.ad_asset_stores_.end(); ++it)
	{
		const Ads_String& s = *it;
		Ads_GUID id = repo->ad_asset_store_id(s.c_str());
		if (ads::entity::is_valid_id(id)) ctx->ad_asset_stores_.push_back(id);
	}
	ctx->user_info_timeout_ = repo->system_config().user_info_timeout_;

	return 0;
}

void Ads_Selector::parse_context_profile(Ads_Selection_Context *ctx)
{
	Ads_Request *req = ctx->request_;
	Ads_Response *res = ctx->response_;
	const Ads_Repository *repo = ctx->repository_;
	if (!req || !res || !repo)
		return;

	//disable server side PSN tracking for comcast ESC-1405 and ip linear
	if (req->is_scte_130_resp())
	{
		if (req->is_comcast_vod_ws1() || req->is_comcast_vod_ws2() || req->is_ip_linear())
		{
			res->enable_vod_tracking_compress_ = false;
		}
	}

	if (ctx->profile_)
	{
		ctx->max_initial_time_position_ = ctx->profile_->max_initial_time_position_;
		// Valid profile level external ad timeout setting
		if (ctx->profile_->external_ad_timeout_ > 0)
		{
			ctx->external_ad_timeout_ = ctx->profile_->external_ad_timeout_;
		}
		// Valid profile level external creative timeout setting
		if (ctx->profile_->external_creative_timeout_ > 0)
		{
			ctx->external_creative_timeout_ = ctx->profile_->external_creative_timeout_;
		}

		bool is_ad_vendor_verification_enabled_in_smart = false;
		bool is_ad_vendor_verification_enabled_in_vast = false;
		for (Ads_String_Pair_RVector::const_iterator it = ctx->profile_->renderer_parameters_.begin(); it != ctx->profile_->renderer_parameters_.end(); ++it)
		{
			const Ads_RString& name = it->first;
			const Ads_RString& value = it->second;
			if (name == "skipOverflowAdInLive")
			{
				ctx->skip_overflow_ad_ = (value == "true");
			}
			else if (name == "PGOnLive")
			{
				ctx->is_pg_td_eligible_on_live_ = (value == "true");
			}
			else if (name == "slotDurationShrinkRatio")
			{
				ctx->slot_duration_shrink_ratio_ = ads::str_to_i64(value.c_str());
			}
			else if (name == "enableServerTranslation")
			{
				ctx->enable_server_translation_ = ads::str_to_i64(value.c_str());
			}
			else if (name == "highResolutionTimePosition")
			{
				ctx->flags_ |= Ads_Selection_Context::FLAG_HIRES_TIME_POSITION;
			}
			else if (name == "bluekaiMobileDataSync")
			{
				ctx->audience_->set_flag(AUDIENCE_FLAG::FLAG_USE_DEVICE_ID);
			}
			else if (name == "spanStyle")
			{
				if (ctx->profile_span_style_.empty()) ctx->profile_span_style_ = value.c_str();
			}
			else if (name == "allow4AId")
			{
				ctx->allow_4A_id_ = true;
			}
			else if (name == "disableAutoEventTracking")
			{
				if (ads::str_to_i64(value.c_str()) == 0)
					ctx->enable_auto_callbacks_ = true;
				else
					ctx->enable_auto_callbacks_ = false;
			}
			else if (name == "disableAudienceData")
			{
				if (value == "1")
					ctx->audience_->set_flag(AUDIENCE_FLAG::FLAG_DISABLE_USER_DB_LOOKUP_BY_PROFILE);
			}
			else if (name == "checkEntertainmentBitrate")
			{
				ctx->check_bitrate_ = true;
			}
			else if (name == "ENABLE_EFFECTIVE_TIMEPOSITION_AND_DURATION_IN_ADS_RESPONSE")
			{
				if (ads::str_to_i64(value.c_str()) > 0)
					ctx->enable_effective_time_position_ = true;
			}
			else if (name == "enableCpxHtmlExpansion") ///OPP-2963, define a switch to control macro #{additional_html} expansion.
			{
				// only set enableCpxHtmlExpansion = 1 explicity can turn this switch on.
				if (value == "1")
					ctx->enable_cpx_html_expansion_ = true;
			}
			else if (name == "enablePriceAwareness")
			{
				if (ads::str_to_i64(value.c_str()) > 0)
					ctx->enable_price_awareness_ = true;
			}
			else if (name == "fwImpressionMap")
			{
				//CPx concrete event list from player profile flags
				get_player_concrete_events(*repo, value.c_str(), ctx->player_concrete_events_);
			}
			else if (name == "jitTranscodePackage")
			{
				Ads_GUID transcode_package_id = ads::entity::make_id(0, ads::str_to_i64(value.c_str()));
				auto it = repo->cch_transcode_packages_.find(transcode_package_id);
				if (it != repo->cch_transcode_packages_.end() && !it->second.empty())
				{
					ctx->jitt().initialize(transcode_package_id, *ctx->profile_);
				}
			}
			else if (name == "enableExternalCandidateSSTF")
			{
				if (ads::str_to_i64(value.c_str()) > 0)
					ctx->enable_external_candidate_sstf_ = true;
			}
			else if (name == "enableProfileMatchingForServerTranslation")
			{
				if (ads::str_to_i64(value.c_str()) > 0)
					ctx->enable_profile_matching_for_server_translation_ = true;
			}
			else if (name == "serverside")
			{
				int val = ads::str_to_i64(value.c_str());
				if (val > 0 && val < 4)
					ctx->server_side_type_ = val;
			}
			else if (name == "enableFakeSlotCallback")
			{
				if (ads::str_to_i64(value.c_str()) == 1
					&& (req->rep()->response_format_ == "vast"
						|| req->rep()->response_format_== "vast2"
						|| req->rep()->response_format_ == "vast2pp"
						|| req->rep()->response_format_ == "vast2ma"
						|| req->rep()->response_format_ == "vast3"
						|| req->rep()->response_format_ == "vast4"))
					ctx->is_fake_slot_callback_enabled_ = true;
			}
			else if (name == "overrideRenditionIdInResponse")
			{
				ctx->override_rendition_id_in_response_ = (value == "1") && req->rep() && (Ads_Response::response_format(ads::tolower(req->rep()->response_format_)) == Ads_Response::NORMAL);
			}

			else if (name == "requestRecordSamplingRate")
			{
				static const int MAX_PROF_PARAM_RT_NUM = 10000;
				int ratio = int(ads::str_to_i64(value.c_str()));
				if (Ads_Server::instance()->is_request_tracking_allowed() && ratio > 0 && ratio <= MAX_PROF_PARAM_RT_NUM)
				{
					if (ctx->rand() % MAX_PROF_PARAM_RT_NUM < ratio)
					{
						req->is_request_tracking_enabled_by_profile_ = true;
					}
				}
			}
			else if (name == "requestRecordSamplingUserID")
			{
				if (Ads_Server::instance()->is_request_tracking_allowed() && req->need_load_ssus())
				{
					std::vector<Ads_String> interested_user_id;
					ads::split(Ads_String(value.c_str()), interested_user_id, ',');
					if (std::find(interested_user_id.begin(), interested_user_id.end(), req->id_repo_.ssus_key()) != interested_user_id.end())
						req->is_request_tracking_enabled_by_profile_ = true;
				}
			}
			else if (name == "scteResponseCreativeDuration")
			{
				req->rep()->log_creative_duration_in_scte_ = (value == "1");
			}
			else if (name == "transmitRawTransactionXML")
			{
				ctx->request_->rep()->canoe_assurance_endpoint_url_ = value.c_str();
			}
			else if (name == "not_support_multiple_impressions" ||
			         name == "support_multiple_impressions_per_time_position_class" ||
			         name == "support_multiple_impressions_per_break")
			{
				auto multiple_impressions_support = Ads_Buyer_Platform::MULTIPLE_IMPRESSIONS_NOT_SUPPORT;
				if (name == "support_multiple_impressions_per_time_position_class")
					multiple_impressions_support = Ads_Buyer_Platform::MULTIPLE_IMPRESSIONS_PER_TIME_POSITION_CLASS;
				else if (name == "support_multiple_impressions_per_break")
					multiple_impressions_support = Ads_Buyer_Platform::MULTIPLE_IMPRESSIONS_PER_BREAK;

				std::vector<Ads_String> items;
				ads::split(Ads_String(value.c_str()), items, ',');
				for (const auto &item : items)
				{
					int buyer_platform_id = atoi(item.c_str());
					auto iter = ctx->support_multi_imps_bps_.find(buyer_platform_id);
					if (iter == ctx->support_multi_imps_bps_.end() || iter->second < multiple_impressions_support)
						ctx->support_multi_imps_bps_[buyer_platform_id] = multiple_impressions_support;
				}
			}
			else if (name == "simultaneousIdLookup")
			{
				// profile level control can be overwritten by CRO NF control.
				if (value == "1")
					ctx->audience_->set_flag(AUDIENCE_FLAG::FLAG_ENABLE_SIMULTANEOUS_ID_LOOKUP);
			}
			else if (name == "nielsen_ott")
			{
				req->is_rbp_nielsen_ott_ = (value == "1");
			}
			else if (name == "platform_group")
			{
				req->rbp_platform_group_ = ads::trim(value.c_str());
			}
			else if (name == "rbpMeasurable")
			{
				ctx->rbp_helper_->init_profile_measurablility_flag(value.c_str());
			}
			else if (name == "IPBasedAudienceTargeting")
			{
				if (value == "1")
					ctx->audience_->set_flag(AUDIENCE_FLAG::FLAG_ENABLE_IP_LOOKUP);
			}
			else if (name == "disableStationQualityCheck")
			{
				ctx->scheduler_->set_flag(SCHEDULER_FLAG::FLAG_DISABLE_STATION_QUALITY_CHECK);
			}
			else if (name == "disableLinearAddressable")
			{
				ADS_DEBUG((LP_DEBUG, "disable linear addressable by profile.\n"));
				req->rep()->set_flag(Ads_Request::Smart_Rep::DISABLE_ADDRESSABLE);
			}
			else if (name == "disableCrossDeviceIdGraph") // profile level cross device control
			{
				if (value == "1")
				{
					ctx->audience_->set_flag(AUDIENCE_FLAG::FLAG_PROFILE_DISABLE_XDEVICE);
				}
			}
			else if (name == "requireSSAICreativeId")
			{
				ctx->require_ssai_creative_id_ = true;
			}
			else if (name == "enableAdVerificationExtensionInSmart")
			{
				is_ad_vendor_verification_enabled_in_smart = (value == "1");
			}
			else if (name == "enableAdVerificationExtensionInVast")
			{
				is_ad_vendor_verification_enabled_in_vast = (value == "1");
			}
			else if (name == "generalizeBidRequestMimeType")
			{
				ctx->generalize_bid_mime_type_ = true;
			}
			else if (name == "requireJiTTEverytime")
			{
				ctx->jitt().set_required_everytime();
			}
			else if (name == "ENABLE_HYLDA_ADS_REQUEST_DATA_INFERENCE")
			{
				//The desired behavior is the following 3 distinct behaviors:
				// no param set - do not invoke inference
				// param value = 1 - only invoke inference if abwct set
				// param value = 2 - invoke inference if abwct set, else use server time
				if (value == "1")
				{
					ctx->scheduler_->set_flag(SCHEDULER_FLAG::FLAG_ENABLE_INFERENCE);
				}
				else if (value == "2")
				{
					ctx->scheduler_->set_flag(SCHEDULER_FLAG::FLAG_ENABLE_INFERENCE);
					ctx->scheduler_->set_flag(SCHEDULER_FLAG::FLAG_INFER_WITHOUT_ABWCT);
				}
				else
				{
					ADS_LOG((LP_ERROR, "wrong parameter value met:%s, will not infer\n", value.c_str()));
				}
			}
			else if (name == "ignoreNetworkPrivacySetting")
			{
				ctx->ignore_network_privacy_setting_ = true;
			}
			else if (name == "forceFallbackAd")
			{
				int num = ads::str_to_i64(value.c_str());
				if (num <= 0 || num >= 10)
					num = 5;
				ctx->force_fallback_ad_num_ = num;
				req->support_frequency_cap_in_vast_by_profile_ = true;
			}
			else if (name == "bypassProfileMatchingForJiTT")
			{
				ctx->bypass_profile_matching_for_jitt_ = true;
			}
			else if (name == "allIPVODParameters")
			{
				if (req->is_viper_csai_vod() && req->vod_router_ != nullptr)
				{
					req->vod_router_->initialize_vod_profile_params(value);
				}
			}
			else if (name == "forwardConnectionAddress")
			{
				ctx->forward_conn_addr_ = true;
			}
			else if (name == "useActualDurationForImprove")
			{
				ctx->use_actual_duration_for_improve_ = (value == "1");
			}
			else if (name == "forceAdRouterNetworkAsVideoCRO")
			{
				ctx->force_ad_router_network_as_video_cro_ = (value == "1");
			}
			else if (name == "enableVODTrackingCompress")
			{
				if (req->is_scte_130_resp())
				{
					res->enable_vod_tracking_compress_ = (value == "1") || (value == "2");
					res->append_ad_info_to_vod_tracking_compress_key_ = (value == "2");
				}
			}
			else if (name == "outputProgrammaticInfo")
			{
				ctx->output_programmatic_info_ = true;
			}
			else if (name == "outputUniversalAdID")
			{
				ctx->output_universal_ad_id_ = true;
			}
			else if (name == "enableCountOnAdEndUEXForSSUS")
			{
				ctx->enable_count_on_ad_end_uex_for_ssus_ = true;
			}
			else if (name == "enableComcastSISLookup")
			{
				// FW-25709: SIS Integration for User Info
				ctx->audience_->set_flag(AUDIENCE_FLAG::FLAG_ENABLE_SIS_LOOKUP_BY_PROFILE);
			}
			else if (name == "enableTrackingUrlRouter")
			{
				if (req->rep() != nullptr)
				{
					req->rep()->capabilities_.disable_tracking_redirect_ = true;
				}
				ctx->enable_tracking_url_router_ = true;
				ctx->opt_out_xff_in_tracking_url_router_ = (value != "2");
			}
			else if (name == "bypassSecondaryContentTypeCheck")
			{
				ctx->bypass_secondary_rendition_asset_content_type_check_ = true;
			}
			else if (name == "enableCountingReplayCallback")
			{
				ctx->enable_counting_replay_callback_ = true;
			}
			else if (name == "enable_mkpl_for_stbvod_downstream_networks")
			{
				std::vector<Ads_String> networks;
				ads::split(Ads_String(value.c_str()), networks, ';');
				for (const Ads_String &nw : networks)
				{
					int64_t n = ads::str_to_i64(nw);
					if (n > 0)
						ctx->stbvod_mkpl_enabled_programmers_.insert(ads::entity::make_id(ADS_ENTITY_TYPE_NETWORK, n));
				}
			}
			else if (name == "optimize_multiround_sstf")
			{
				ctx->multiround_sstf_optimization_enabled_ = true;
				ctx->external_candidate_max_load_rounds_ =  repo->system_config().external_candidate_optimized_max_load_rounds_;
			}
			else if (name == "autoloadExtensions")
			{
				req->enable_auto_load_extensions_ = true;
			}
		}

		Ads_Response::FORMAT fmt = Ads_Response::response_format(req->rep()->response_format_);
		ctx->is_ad_vendor_verification_enabled_ = (Ads_Response::vast_version(fmt) >= Ads_Response::VAST_V4) ||
							(is_ad_vendor_verification_enabled_in_smart && fmt == Ads_Response::NORMAL) ||
							(is_ad_vendor_verification_enabled_in_vast && Ads_Response::is_vast(fmt));

		//CPx support concrete events
		for (Ads_GUID_RSet::const_iterator cit = ctx->profile_->supported_concrete_events_.begin(); cit != ctx->profile_->supported_concrete_events_.end(); ++cit)
		{
			ctx->supported_concrete_events_.insert(*cit);
		}

		if (repo->system_config().is_profile_aerospike_write_queue_enabled(ctx->profile_->id_) ||
			Ads_Server_Config::instance()->is_profile_aerospike_write_queue_enabled(ads::entity::id(ctx->profile_->id_)) )
		{
			res->enable_as_write_queue();
		}
	}
}

bool
Ads_Selector::test_advertisement_refresh_exclusivity(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate *target, const Ads_Slot_Base *slot)
{
	if (!ctx->request_->rep()->capabilities_.refresh_display_slots_ ||
		!(ctx->refresh_config_ || ctx->request_->rep()->capabilities_.refresh_display_on_demand_)) return true;

	const auto it = ctx->user_->display_refresh_history_.find(slot->custom_id_);
	if (it == ctx->user_->display_refresh_history_.end()) return true;

	for (Ads_GUID_List::iterator itor = it->second.begin(); itor != it->second.end(); ++itor)
	{
		if (*itor == target->ad_->id_)
		{
			ADS_DEBUG((LP_DEBUG, "exclude ad %s in display refresh history\n", CANDIDATE_LITE_ID_CSTR(target->lite_)));
			return false;
		}
	}
	return true;
}

bool
Ads_Selector::test_advertisement_unit_sponsorship(const Ads_Advertisement_Candidate_Lite* candidate, const Ads_Slot_Base *slot, bool verbose) const
{
	// Forward to Ads_Advertisement_Candidate_Lite
	return candidate->test_advertisement_unit_sponsorship(slot, verbose);
}

void
Ads_Selector::add_advertisement_to_blacklist(Ads_GUID ad_id)
{
	Ads_GUID_Set_Ptr l(new Ads_GUID_Set());
	if (this->advertisement_blacklist_)
	{
		std::copy(this->advertisement_blacklist_->begin(), this->advertisement_blacklist_->end(),
			std::inserter(*l, l->begin()));
	}

	l->insert(ad_id);
	this->advertisement_blacklist_ = l;
}

void
Ads_Selector::remove_advertisement_from_blacklist(Ads_GUID ad_id)
{
	Ads_GUID_Set_Ptr l(new Ads_GUID_Set());
	if (this->advertisement_blacklist_)
	{
		std::copy(this->advertisement_blacklist_->begin(), this->advertisement_blacklist_->end(),
			std::inserter(*l, l->begin()));
	}
	l->erase(ad_id);

	this->advertisement_blacklist_ = l;
}

void
Ads_Selector::clear_advertisement_blacklist()
{
	Ads_GUID_Set_Ptr l(new Ads_GUID_Set());
	this->advertisement_blacklist_ = l;
}

Ads_String
Ads_Selector::get_advertisement_blacklist()
{
	Ads_String s;
	if (!this->advertisement_blacklist_) return s;

	for (Ads_GUID_Set::const_iterator it = this->advertisement_blacklist_->begin(); it != this->advertisement_blacklist_->end(); ++it)
	{
		Ads_GUID ad_id = *it;
		int replica_id = (int)ads::ad::replica_id(ad_id);
		s += ads::entity::str(ad_id);
		if (replica_id > 0)
		{
			s += ":";
			s += ads::i64_to_str(replica_id);
		}
		s += ",";
	}

	return s;
}

Ads_String
Ads_Selector::get_advertisement_warnings()
{
	Ads_String s;

	ads::Guard __g(this->invalid_advertisement_states_mutex_);
	for (std::map<Ads_GUID, time_t>::const_iterator it = this->invalid_advertisement_states_.begin();
		it != this->invalid_advertisement_states_.end();
		++it )
	{
		Ads_GUID id = it->first;
		time_t t = it->second;

		s += ADS_ENTITY_ID_STR(id);
		s += ",";
		s += (ads::entity::type(id) == ADS_ENTITY_TYPE_ADVERTISEMENT ? "AD" : "CR");
		s += ",";
		s += ads::i64_to_str(t);
		s += ";";
	}

	return s;
}

bool Ads_MRM_Rule_Path::has_higher_priority_than_candidate(Ads_Selection_Context *ctx, Ads_Slot_Base *slot, const Ads_Advertisement_Candidate *candidate) const
{
	auto assoc = slot->associate_type();
	if (!candidate  || candidate->ad_->is_house_ad(ctx->root_network_id(assoc)))
	{
		return true;
	}
	if ((candidate->is_sponsorship(slot) && candidate->ad_ && candidate->ad_->is_sponsorship()) ||
		candidate->is_scheduled_in_slot(slot))
	{
		return false;
	}

	auto path_priorities = Ads_MRM_Rule_Path::calculate_priorities(ctx->repository_, this);
	auto ad_priorities = candidate->priority(slot);

	if (ad_priorities && !path_priorities->priorities_.empty())
	{
		if (ad_priorities[0] == path_priorities->priorities_[0])
		{
			return (!candidate->is_guaranteed(slot) || path_priorities->guaranteed_ad_eligible_);
		}
		return path_priorities->priorities_[0] > ad_priorities[0];
	}
	ADS_LOG((LP_ERROR, "priority couldn't be empty: \n\tcandidate(%s) %s empty,\n\tpath priority %s empty:%s\n",
			 CANDIDATE_AD_LPID_CSTR(candidate), ad_priorities ? "isn't" : "is",
			 path_priorities->priorities_.empty() ? "is" : "isn't", this->path_string().c_str()));
	return false;
}

bool
Ads_Selector::greater_advertisement_candidate(Ads_Selection_Context* ctx, ads::MEDIA_TYPE assoc, const Ads_Advertisement_Candidate* x, const Ads_Advertisement_Candidate* y, bool priority_only, bool ignore_duration /* = false */, bool ignore_active)
{
	if (x->is_pod_ad() && y->is_pod_ad()
		&& x->get_leader_pod_ad() == y->get_leader_pod_ad())
	{
		return x->pod_replica_id() < y->pod_replica_id();
	}

	const Ads_Advertisement_Candidate *leader_pod_x = x->get_leader_pod_ad();
	if (leader_pod_x != nullptr)
		x = leader_pod_x;
	const Ads_Advertisement_Candidate* leader_pod_y = y->get_leader_pod_ad();
	if (leader_pod_y != nullptr)
		y = leader_pod_y;

	if (!priority_only && !ignore_active)
	{
		//inactive candidates
		if (!x->active(assoc))
		{
			return false;
		}
		else if (!y->active(assoc))
		{
			return true;
		}

		ADS_ASSERT(x->active(assoc) && y->active(assoc));
	}

	if (x->filled_selection_round_ < y->filled_selection_round_)
		return true;
	else if (y->filled_selection_round_ < x->filled_selection_round_)
		return false;

	if (x->is_sponsorship(assoc) && x->ad_ && x->ad_->is_sponsorship())
	{
		if (y->is_sponsorship(assoc) && y->ad_ && y->ad_->is_sponsorship())
		{
			int64_t x_position_in_slot = x->ad_->ad_unit() ? x->ad_->ad_unit()->position_in_slot_ : 0;
			int64_t y_position_in_slot = y->ad_->ad_unit() ? y->ad_->ad_unit()->position_in_slot_ : 0;
			return (x->ad_->priority_ > y->ad_->priority_)?true:
				(x->ad_->priority_ < y->ad_->priority_)?false:
				(x_position_in_slot < 0 && y_position_in_slot >= 0)?false:
				(x_position_in_slot >= 0 && y_position_in_slot < 0)?true:
				(x_position_in_slot < y_position_in_slot)?true:

				(x->competing_eCPM(assoc) > y->competing_eCPM(assoc))?true:
						(assoc == ads::MEDIA_VIDEO)?false:
					x->ad_->width() * x->ad_->height() > y->ad_->width() * y->ad_->height();
		}
		return true;
	}
	else if (y->is_sponsorship(assoc) && y->ad_ && y->ad_->is_sponsorship())
		return false;

	/// MRM-7232 checking house-ad before rule priority
	if (!priority_only)
	{
		if (x->ad_->is_house_ad(ctx->root_network_id(assoc)))
		{
			if (!y->ad_->is_house_ad(ctx->root_network_id(assoc))) return false;
			else
			{
				if (x->ad_->priority_ > y->ad_->priority_) return true;
				else if (x->ad_->priority_ < y->ad_->priority_) return false;
			}
		}
		else
		{
			if (y->ad_->is_house_ad(ctx->root_network_id(assoc))) return true;
		}
	}

	///TODO: makeup
	// check prorioty
	const double *x_priority = x->priority(assoc);
	const double *y_priority = y->priority(assoc);

	if (x_priority && y_priority)
	{
		size_t num_priority = 1; // std::min(x->num_priority(assoc), y->num_priority(assoc));
		for (size_t i = 0; i < num_priority; ++i)
		{
			if (x_priority[i] > y_priority[i]) return true;
			else if (x_priority[i] < y_priority[i]) return false;
			else if (!priority_only)
			{
				if (x->ad_->is_house_ad())
				{
					if (!y->ad_->is_house_ad()) return false;
					else
					{
						if (x->ad_->priority_ > y->ad_->priority_) return true;
						else if (x->ad_->priority_ < y->ad_->priority_) return false;
					}
				}
				else
					if (y->ad_->is_house_ad()) return true;
			}
			// compare sub priority betweens ads from HG rule
			auto x_relative_priority = x_priority[i] > 0 ? x->relative_priority_in_guaranteed_bucket() : 0;
			auto y_relative_priority = x_priority[i] > 0 ? y->relative_priority_in_guaranteed_bucket() : 0;
			if (x_relative_priority != y_relative_priority)
				return x_relative_priority > y_relative_priority;
		}
	}
	else if (!y_priority)
		return true;
	else
		return false;

	//check for exempt and sponsorship
	if (x->is_sponsorship(assoc) || x->is_guaranteed(assoc))
	{
		if (!y->is_sponsorship(assoc) && !y->is_guaranteed(assoc)) return true;

		// goes here means both x and y are guaranteed, handle sponsorship
		if (x->ad_ && x->ad_->is_sponsorship())
		{
			int64_t x_position_in_slot = x->ad_->ad_unit() ? x->ad_->ad_unit()->position_in_slot_ : 0;
			int64_t y_position_in_slot = y->ad_->ad_unit() ? y->ad_->ad_unit()->position_in_slot_ : 0;
			if (y->ad_ && y->ad_->is_sponsorship())
			{
				return (x->ad_->priority_ > y->ad_->priority_)?true:
				       (x->ad_->priority_ < y->ad_->priority_)?false:
				       (x_position_in_slot < 0 && y_position_in_slot >= 0)?false:
				       (x_position_in_slot >= 0 && y_position_in_slot < 0)?true:
 				       (x_position_in_slot < y_position_in_slot)?true:
				       (x->competing_eCPM(assoc) > y->competing_eCPM(assoc));
			}
			return true;
		}
		else
		{
			if (y->ad_ && y->ad_->is_sponsorship()) return false;
		}

		// goes here means both x and y are guaranteed & not sponsorsip, compare fill rate
		if (x->ad_ && x->ad_->is_exempt())
		{
			if (y->ad_ && y->ad_->is_exempt())
			return x->ad_->schedule_mode_ > y->ad_->schedule_mode_ ? true:
				x->ad_->schedule_mode_ < y->ad_->schedule_mode_ ? false:
				x->ad_->priority_ > y->ad_->priority_? true: //if both EXEMPT, compare priority(11-20) first then fill rate
				x->ad_->priority_ < y->ad_->priority_? false:
				(x->daily_fill_rate_ >= 0 && x->daily_fill_rate_ < y->daily_fill_rate_)? true:
				(y->daily_fill_rate_ >= 0 && x->daily_fill_rate_ > y->daily_fill_rate_)? false:
				x->fill_rate_ < y->fill_rate_;
			return true;
		}
		else
		{
			if (y->ad_ && y->ad_->is_exempt()) return false;
		}
	}
	else if (y->is_sponsorship(assoc) || y->is_guaranteed(assoc))
	{
		return false;
	}

	// compare buyer priority for preemptible ads first
	auto x_buyer_priority = x->buyer_priority(assoc);
	auto y_buyer_priority = y->buyer_priority(assoc);
	if (x_buyer_priority != y_buyer_priority)
	{
		return x_buyer_priority < y_buyer_priority; // smaller priority value means greater
	}

	if (priority_only)
	{
		return false;
	}

	// meet schedule?
#if defined (ADS_ENABLE_FORECAST)
	if (!(ctx->request_flags() & Ads_Request::Smart_Rep::PROPOSAL_IF_NO_BUDGET))
#endif
	{
		if (x->meet_schedule())
		{
			if (!y->meet_schedule())
				return false;
			else if (assoc != ads::MEDIA_VIDEO && x->ad_ && y->ad_)
				return x->ad_->width() * x->ad_->height() > y->ad_->width() * y->ad_->height();
			else
				return (ctx->request_flags() & Ads_Request::Smart_Rep::DONT_SHUFFLE_CANDIDATES) ? (x->ad_->id_ > y->ad_->id_)
					: (x > y);	//XXX: (FDB-2988) all met schedule ads are equal
		}
		else if (y->meet_schedule())
		{
			return true;
		}
	}

	// goes here means neither x nor y is a behind schedule guaranteed ad
	// compare profit then
	if (assoc == ads::MEDIA_VIDEO)
	{
		if (ignore_duration || (!x->override_duration_ && !y->override_duration_))
			return x->competing_eCPM(assoc) > y->competing_eCPM(assoc);
		else if (!x->override_duration_)
			return false;
		else if (!y->override_duration_)
			return true;
		return (double) x->competing_eCPM(assoc) / x->override_duration_ > (double) y->competing_eCPM(assoc) / y->override_duration_;
	}
	else
	{
		return x->competing_eCPM(assoc) > y->competing_eCPM(assoc)? true
		: x->competing_eCPM(assoc) < y->competing_eCPM(assoc)? false
		: x->ad_->width() * x->ad_->height() > y->ad_->width() * y->ad_->height();
	}
}

bool
Ads_Selector::reseller_optimized_greater_advertisement_candidate(Ads_Selection_Context* ctx, ads::MEDIA_TYPE assoc, const Ads_Advertisement_Candidate* x, const Ads_Advertisement_Candidate* y, bool ignore_duration)
{
	auto *x_closure = x->owner_.to_closure();
	auto *y_closure = y->owner_.to_closure();
	if (!x_closure || !y_closure || (x_closure->network_id_ != y_closure->network_id_))
		return false;

	// pod ad, use the lead ad in pod to compare
	if (x->is_pod_ad() && y->is_pod_ad()
		&& x->get_leader_pod_ad() == y->get_leader_pod_ad())
	{
		return x->pod_replica_id() < y->pod_replica_id();
	}

	const Ads_Advertisement_Candidate *leader_pod_x = x->get_leader_pod_ad();
	if (leader_pod_x != nullptr)
		x = leader_pod_x;
	const Ads_Advertisement_Candidate* leader_pod_y = y->get_leader_pod_ad();
	if (leader_pod_y != nullptr)
		y = leader_pod_y;

	// sponsor ad
	if (x->ad_ && x->ad_->is_sponsorship())
	{
		if (y->ad_ && y->ad_->is_sponsorship())
		{
			int64_t x_position_in_slot = x->ad_->ad_unit() ? x->ad_->ad_unit()->position_in_slot_ : 0;
			int64_t y_position_in_slot = y->ad_->ad_unit() ? y->ad_->ad_unit()->position_in_slot_ : 0;
			return (x->ad_->priority_ > y->ad_->priority_)?true:
				(x->ad_->priority_ < y->ad_->priority_)?false:
				(x_position_in_slot < 0 && y_position_in_slot >= 0)?false:
				(x_position_in_slot >= 0 && y_position_in_slot < 0)?true:
				(x_position_in_slot < y_position_in_slot)?true:
				(x->internal_competing_eCPM(assoc) > y->internal_competing_eCPM(assoc))?true:
						(assoc == ads::MEDIA_VIDEO)?false:
					x->ad_->width() * x->ad_->height() > y->ad_->width() * y->ad_->height();
		}
		return true;
	}
	else if (y->ad_ && y->ad_->is_sponsorship())
		return false;

	// house ad
	if (x->ad_->is_house_ad())
	{
		if (!y->ad_->is_house_ad())
			return false;
		else
		{
			if (x->ad_->priority_ > y->ad_->priority_)
				return true;
			else if (x->ad_->priority_ < y->ad_->priority_)
				return false;
		}
	}
	else if (y->ad_->is_house_ad())
		return true;

	// above paying
	if (x->ad_ && x->is_guaranteed())
	{
		if (!y->ad_ || !y->is_guaranteed())
			return true;

		if (x->ad_ && x->ad_->is_exempt())
		{
			if (y->ad_ && y->ad_->is_exempt())
			return x->ad_->schedule_mode_ > y->ad_->schedule_mode_ ? true:
				x->ad_->schedule_mode_ < y->ad_->schedule_mode_ ? false:
				x->ad_->priority_ > y->ad_->priority_? true: //if both EXEMPT, compare priority(11-20) first then fill rate
				x->ad_->priority_ < y->ad_->priority_? false:
				(x->daily_fill_rate_ >= 0 && x->daily_fill_rate_ < y->daily_fill_rate_)? true:
				(y->daily_fill_rate_ >= 0 && x->daily_fill_rate_ > y->daily_fill_rate_)? false:
				x->fill_rate_ < y->fill_rate_;
			return true;
		}
		else if (y->ad_ && y->ad_->is_exempt())
			return false;
	}
	else if (y->ad_ && y->is_guaranteed())
		return false;

	// meet schedule
	if (x->meet_schedule())
	{
		if (!y->meet_schedule())
			return false;
		else if (assoc != ads::MEDIA_VIDEO && x->ad_ && y->ad_)
			return x->ad_->width() * x->ad_->height() > y->ad_->width() * y->ad_->height();
		else
			return (ctx->request_flags() & Ads_Request::Smart_Rep::DONT_SHUFFLE_CANDIDATES) ? (x->ad_->id_ > y->ad_->id_)
				: (x > y);	//XXX: (FDB-2988) all met schedule ads are equal
	}
	else if (y->meet_schedule())
	{
		return true;
	}

	if (assoc == ads::MEDIA_VIDEO)
	{
		if (ignore_duration || (!x->override_duration_ && !y->override_duration_))
			return x->internal_competing_eCPM(assoc) > y->internal_competing_eCPM(assoc);
		else if (!x->override_duration_)
			return false;
		else if (!y->override_duration_)
			return true;

		return (double) x->internal_competing_eCPM(assoc) / x->override_duration_ > (double) y->internal_competing_eCPM(assoc) / y->override_duration_;
	}
	else
	{
		return x->internal_competing_eCPM(assoc) > y->internal_competing_eCPM(assoc) ? true
			: x->internal_competing_eCPM(assoc) < y->internal_competing_eCPM(assoc) ? false
			: x->ad_->width() * x->ad_->height() > y->ad_->width() * y->ad_->height();
	}
}

bool
Ads_Selector::is_reseller_optimized_ad_ranking_applicable(const Ads_Selection_Context *ctx,
							const Ads_Advertisement_Candidate& candidate,
							int assoc /* = ads::MEDIA_ALL */) const
{
	auto *closure = candidate.owner_.to_closure();
	if (candidate.ad_ == nullptr || candidate.ad_->is_external_ || closure == nullptr)
		return false;

	assoc &= candidate.assoc_;
	if ((!(assoc & ads::MEDIA_VIDEO) || ctx->root_asset_ == nullptr || ctx->root_asset_->network_id_ == closure->network_id_) &&
		(!(assoc & ads::MEDIA_SITE_SECTION) || ctx->root_section_ == nullptr || ctx->root_section_->network_id_ == closure->network_id_))
	{
		return false;
	}

	return ctx->repository_->system_config().is_reseller_optimized_ad_ranking_network(candidate.ad_->network_id_);
}

int
Ads_Selector::apply_reseller_optimized_ad_ranking(Ads_Selection_Context *ctx, ads::MEDIA_TYPE assoc)
{
	if (!ctx->repository_->system_config().has_reseller_optimized_ad_ranking_networks())
		return 0;

	std::map<const selection::Advertisement_Owner*, std::pair<std::vector<size_t>, Ads_Advertisement_Candidate_Vector> > closure_candidates_info;
	for (size_t pos = 0; pos < ctx->final_candidates_.size(); ++pos)
	{
		auto *candidate = ctx->final_candidates_[pos];
		if (!is_reseller_optimized_ad_ranking_applicable(ctx, *candidate, assoc))
		{
			continue;
		}

		auto& candidates_info = closure_candidates_info[&candidate->owner_];
		candidates_info.first.push_back(pos);
		candidates_info.second.push_back(candidate);
	}

	for (auto &kv : closure_candidates_info)
	{
		const auto& positions = kv.second.first;
		auto& candidates = kv.second.second;

		std::stable_sort(candidates.begin(), candidates.end(), Ads_Advertisement_Candidate::reseller_optimized_greater(ctx, assoc));
		if (Ads_Server_Config::instance()->enable_debug_)
		{
			ADS_DEBUG((LP_TRACE, "\nmaintain %s ad ranking for ads: \n", kv.first->tag_str().c_str()));
			for (const auto* candidate : candidates)
			{
				ADS_DEBUG((LP_TRACE, "ad: %s\n", ADS_ADVERTISEMENT_ID_CSTR(candidate->ad_->id_)));
			}
		}
		ADS_ASSERT(positions.size() == candidates.size());

		auto it = candidates.begin();
		for (size_t pos : positions)
		{
			ADS_ASSERT(it != candidates.end());
			ctx->final_candidates_[pos] = *it;
			++it;
		}
	}

	return closure_candidates_info.size();
}

int Ads_Selector::dump_candidate_slots_assignment_info(Ads_Selection_Context &ctx, ads::MEDIA_TYPE assoc)
{
	if (assoc == ads::MEDIA_SITE_SECTION)
	{
		ADS_DEBUG((LP_DEBUG, "Sorting ads for display slots (by profit of site section owners)...\n"));
	}
	else if (assoc == ads::MEDIA_VIDEO)
	{
		ADS_DEBUG((LP_DEBUG, "Sorting ads for video/player slots (by profit of video owners)...\n"));
	}
	else
	{
		ADS_DEBUG((LP_DEBUG, "No need to output ad slots info\n"));
		return -1;
	}
	for (size_t i = 0; i < ctx.final_candidates_.size(); ++i)
	{
		Ads_Advertisement_Candidate* candidate = ctx.final_candidates_[i];
		if (candidate == nullptr || candidate->lite_ == nullptr)
			continue;
		if (!candidate->active(assoc))
			continue;

		for (auto it = candidate->slot_references_.begin(); it != candidate->slot_references_.end(); ++it)
		{
			const Ads_Slot_Base *slot = it->first;
			if (slot == nullptr)
				continue;
			const Ads_Advertisement_Candidate::Slot_Ref_Ptr& slot_ref = it->second;
			if (slot_ref == nullptr)
				continue;
			if (assoc == ads::MEDIA_VIDEO)
			{
				if (candidate->lite_->is_watched())
				{
					//add the ad's related info to the response
					Ads_String msg = "effective_eCPM:" + ads::i64_to_str(candidate->effective_eCPM(slot)) + ", competing_eCPM:" + ads::i64_to_str(candidate->competing_eCPM(slot));
					ctx.log_watched_info(*candidate->lite_, slot, msg);
				}
			}
			if (slot->associate_type() != assoc)
				continue;
			const auto fill_rate = candidate->lite_->fill_rate(ctx);
			ADS_DEBUG((LP_DEBUG, "    Ad %s Slot: %s, Assoc:%s, %s %sFill Rate:%.3f, Time:%.3f, Daily Goal Fill Rate: %d, "
			"Rule Fill Rate: %d, Effective eCPM:%s, Competing eCPM:%s, Duration:%d, Override Duration:%d, Net eCPM:%s\n",
					CANDIDATE_LITE_LPID_CSTR(candidate->lite_),
					slot->custom_id_.c_str(),
					ads::media_name(slot->associate_type()),
					candidate->is_guaranteed(slot)? "Guaranteed, " : "",
					candidate->soft_guaranteed_version_ == Ads_Advertisement_Candidate::SOFT_GUARANTEED_VERSION::V1 ? "Soft Guaranteed, " : "",
					fill_rate.fill_rate_,
					fill_rate.time_,
					candidate->daily_fill_rate_,
					candidate->rule_fill_rate(slot),
					ads::i64_to_str(slot_ref->effective_eCPM_).c_str(),
					ads::i64_to_str(candidate->competing_eCPM(slot)).c_str(),
					static_cast<int>(candidate->duration(slot)),
					candidate->override_duration_,
					ads::i64_to_str(slot_ref->net_eCPM().bidding_eCPM_).c_str()
			));
		}
	}
	ADS_DEBUG((LP_DEBUG, "\n"));
	return 0;
}

bool
Ads_MRM_Rule_Path::is_partner_guaranteed_ad_eligible(const Ads_Repository *repo, const Ads_MRM_Rule *rule, Ads_GUID partner_network_id)
{
	if (!rule || !rule->enable_partner_guaranteed_)
		return false;

	if (!repo->is_partner_guaranteed_enabled(rule->network_id_, partner_network_id))
		return false;
	//	ADS_DEBUG((LP_DEBUG, "ad %s rule path is partner guaranteed\n", CANDIDATE_LITE_LPID_CSTR(candidate->lite_)));
	return true;
}

std::shared_ptr<Ads_Inventory_Access_Path::Priority>
Ads_MRM_Rule_Path::calculate_priorities(const Ads_Repository *repo, const Ads_MRM_Rule_Path *path,
					const Ads_MRM_Rule *external_rule, Ads_GUID external_network_id)
{
	std::vector<std::pair<const Ads_MRM_Rule*, Ads_GUID>> rule_partners;
	rule_partners.reserve((path != nullptr ? path->edges_.size() : 0) +
				(external_rule != nullptr ? 1 : 0));

	if (path != nullptr)
	{
		for (const auto& edge : path->edges_)
		{
			ADS_ASSERT(edge.rule_ != nullptr);
			rule_partners.emplace_back(edge.rule_, edge.partner_id());
		}
	}
	if (external_rule != nullptr)
	{
		rule_partners.emplace_back(external_rule, external_network_id);
	}

	double step = 1.0, current = 0;
	bool started = false;
	auto  path_priorities = std::make_shared<Priority>();
	std::vector<double> &priority = path_priorities->priorities_;
	Ads_GUID_Vector &networks = path_priorities->priority_base_networks_;

	Ads_GUID current_network_id = -1;
	bool last_rule_recorded = false;
	const Ads_MRM_Rule *rule = nullptr;

	for (auto &rule_partner : rule_partners)
	{
		last_rule_recorded = false;
		rule = rule_partner.first;

		if (!started)
		{
			started = true;
			current_network_id = rule->network_id_;
		}

		Ads_GUID partner_network_id = rule_partner.second;

		switch (rule->priority_)
		{
			case Ads_MRM_Rule::HARD_GUARANTEED:
			case Ads_MRM_Rule::YOU_FIRST:
				step /= 2;
				current += step;
				break;

			case Ads_MRM_Rule::ME_FIRST:
			{
				int rp = rule->get_reseller_priority(ads::entity::restore_looped_id(partner_network_id));
				rp = std::min(100, rp);  // avoid divided by zero;
				step /= (101 - rp);
				current -= step;
				if (!rule->is_old_application_rule())
				{
					priority.emplace_back(current);
					networks.emplace_back(ads::entity::restore_looped_id(partner_network_id));
					last_rule_recorded = true;
				}
			}
				break;
			case Ads_MRM_Rule::OPEN_COMPETITION:
			case Ads_MRM_Rule::SOFT_GUARANTEED:
			{
				priority.emplace_back(current);
				networks.emplace_back(current_network_id);
				current_network_id = ads::entity::restore_looped_id(partner_network_id);
				current = 0;
				step = 1.0;
			}
				break;

			default:
				ADS_ASSERT(0);
				break;
		}
	}

	if (!last_rule_recorded)
	{
		priority.emplace_back(current);
		networks.emplace_back(current_network_id);
	}

	if (path == nullptr || path->edges_.empty() || path->committed_through())
	{
		path_priorities->guaranteed_ad_eligible_ = (external_rule == nullptr ||
							(rule != nullptr && rule->deal_ != nullptr && rule->deal_->is_guaranteed_deal()));
	}
	else if (path->edges_.back().network_id() == path->committed_network_id_)
	{
		path_priorities->guaranteed_ad_eligible_ = (external_rule == nullptr &&
								is_partner_guaranteed_ad_eligible(repo, rule,
									ads::entity::restore_looped_id(path->edges_.back().partner_id())));
	}

	return path_priorities;
}

int
Ads_Selector::update_advertisement_candidate_priority(const Ads_Selection_Context* ctx, Ads_Advertisement_Candidate* candidate, const Ads_Slot_Base* slot)
{
	ADS_ASSERT(ctx && ctx->repository_);
	const auto* repo = ctx->repository_;

	const auto &path = candidate->access_path(slot);
	double priority_value_base = 0;
	bool ignore_rule_path = false;
	bool ignore_external_rule = false;
	const Ads_Deal *mkpl_prgm_deal = nullptr;

	if (path.mkpl_path_ != nullptr)
	{
		const auto &final_buyer = path.mkpl_path_->final_buyer();
		const auto &host_node = final_buyer.hosting_execution_node();
		if (final_buyer.inbound_node() == &host_node)
			mkpl_prgm_deal = final_buyer.inbound_order_->order_.associated_deal_;

		if (&candidate->operation_owner() != &host_node.to_ad_owner())
		{
			ignore_external_rule = true;
			candidate->guaranteed_eligible_ = false;
		}

		if (!host_node.is_root_seller())
		{
			priority_value_base = path.mkpl_path_->priority_base();
			ignore_rule_path = true;
		}
	}
	
	const Ads_MRM_Rule *external_rule = nullptr;
	Ads_MRM_Rule mkpl_prgm_faked_rule;
	if (mkpl_prgm_deal != nullptr)
	{
		mkpl_prgm_faked_rule.deal_ = mkpl_prgm_deal;
		switch (mkpl_prgm_deal->type_)
		{
		case Ads_Deal::FIRST_LOOK:
			mkpl_prgm_faked_rule.priority_ = Ads_MRM_Rule::YOU_FIRST;
			break;
		case Ads_Deal::BACKFILL:
			mkpl_prgm_faked_rule.priority_ =  Ads_MRM_Rule::ME_FIRST;
			break;
		default:
			mkpl_prgm_faked_rule.priority_ =  Ads_MRM_Rule::OPEN_COMPETITION;
			break;
		}

		external_rule = &mkpl_prgm_faked_rule;
		ADS_ASSERT(!ads::entity::is_valid_id(candidate->external_rule_id(slot)));
	}
	else if (!ignore_external_rule)
	{
		const Ads_Asset_Section_Closure* closure = nullptr;
		Ads_GUID external_rule_id = candidate->external_rule_id(slot);
		if (ads::entity::is_valid_id(external_rule_id))
		{
			if (repo->find_access_rule(external_rule_id, external_rule) < 0 || external_rule == nullptr)
				return -1;
		}
		else if (candidate->ad_->is_portfolio() &&
                         (path.mrm_rule_path_ == nullptr || path.mrm_rule_path_->edges_.empty()) &&
			(closure = candidate->owner_.to_closure()) != nullptr)
		{
			auto sit = closure->slot_references_.find(slot);
			if (sit != closure->slot_references_.end())
			{
				const Ads_Asset_Section_Closure::Slot_Ref& slot_ref = sit->second;
				for (const auto &src_pr : slot_ref.sources_)
				{
					Ads_GUID network_id = src_pr.first;
					const Ads_Asset_Section_Closure::Slot_Ref::Source &source = src_pr.second;
					const Ads_Asset_Section_Closure* cro = ctx->closure(network_id);
					if (cro != nullptr && cro->guaranteed(slot))
					{
						repo->find_access_rule(source.access_rule_id_, external_rule);
						break;
					}
				}
			}
		}
	}

	auto path_priorities = Ads_MRM_Rule_Path::calculate_priorities(repo, (!ignore_rule_path ? path.mrm_rule_path_ : nullptr),
									external_rule, candidate->get_network_id());

	size_t count = 0;
	while (count < path_priorities->priorities_.size() && count < Ads_Advertisement_Candidate::NUM_PRIORITY_CASCADE)
	{
		candidate->priority(slot)[count] = priority_value_base + path_priorities->priorities_[count];
		candidate->priority_base(slot)[count] =  path_priorities->priority_base_networks_[count];
		++count;
	}

	candidate->num_priority(slot) = count;
	candidate->guaranteed_eligible_ = candidate->guaranteed_eligible_ && path_priorities->guaranteed_ad_eligible_;
	return 0;
}

int
Ads_Selector::check_user_experience_commercial_ratio(Ads_Selection_Context *ctx, Ads_Request *req, bool check_uex)
{
	Ads_User_Experience_Controller * uec = Ads_User_Experience_Controller::get_controller(ctx);
	if (uec == nullptr)
	{
		ADS_DEBUG((LP_DEBUG, "no UEX config found for the req\n"));
		return 0;
	}
	ADS_DEBUG((LP_DEBUG, "using UEX config %s.\n", ADS_ENTITY_ID_CSTR(uec->ux_conf_->id_)));
	ctx->ux_conf_ = uec->ux_conf_;
	ctx->ux_network_id_ = uec->ux_network_->id_;
	ctx->ux_inventory_id_ = ads::entity::type(uec->ux_conf_->id_) == ADS_ENTITY_TYPE_ASSET || ads::entity::type(uec->ux_conf_->id_) == ADS_ENTITY_TYPE_AUDIENCE ? uec->ux_conf_->id_ : ctx->user_->website_root_id();

	if (check_uex)
		uec->apply(ctx, req);
	if (uec != nullptr)
	{
		delete uec;
		uec = nullptr;
	}
	return 0;
}


Ads_Creative_Rendition *
Ads_Request::Smart_Rep::Ad_Set::Ad::Creative::to_entity(const Ads_Repository *repo) const
{
	Ads_Creative_Rendition *rendition = ads::new_object<Ads_Creative_Rendition>();
	rendition->id_ = ads::entity::make_id(ADS_ENTITY_TYPE_CREATIVE_RENDITION, 0);
	rendition->creative_id_ = ads::entity::make_id(ADS_ENTITY_TYPE_CREATIVE, 0);

	if (!this->content_type_.empty()) rendition->content_type_ = this->content_type_.c_str();
	if (!this->width_.empty()) rendition->width_ = ads::str_to_i64(this->width_.c_str());
	if (!this->height_.empty()) rendition->height_ = ads::str_to_i64(this->height_.c_str());
	if (!this->creative_api_.empty()) rendition->creative_api_ = this->creative_api_.c_str();

	if (!this->parameters_.empty())
	{
		for (std::list<Ads_String_Pair >::const_iterator it = this->parameters_.begin(); it != this->parameters_.end(); ++it)
			rendition->rendition_parameters_.push_back(std::make_pair(it->first.c_str(), it->second.c_str()));
	}

	if (!this->event_callbacks_.empty())
	{
		rendition->has_action_ = true;
		for (std::list<Event_Callback *>::const_iterator it = this->event_callbacks_.begin(); it != this->event_callbacks_.end(); ++it)
		{
			const Event_Callback *ec = *it;

			Ads_Creative_Rendition::URL_TYPE ut = Ads_Creative_Rendition::URL_IMPRESSION;
			if (ec->type_ == "i") ut = Ads_Creative_Rendition::URL_IMPRESSION;
			else if (ec->type_ == "c") ut = Ads_Creative_Rendition::URL_CLICK;
			else if (ec->type_ == "a") ut = Ads_Creative_Rendition::URL_ACTION;
			else if (ec->type_ == "s") ut = Ads_Creative_Rendition::URL_STANDARD;

			Ads_Creative_Rendition::Action *act = ads::new_object<Ads_Creative_Rendition::Action>();
			act->name_ = ec->name_.c_str();
			act->url_ = ec->redirect_.c_str();
			if (ec->type_ == "c" && act->name_ == CALLBACK_NAME_DEFAULT_CLICK)
				act->is_click_through_ = true;

			rendition->actions_[ut].push_back(act);
		}
	}

	if (this->asset_.valid_)
	{
		Ads_Creative_Rendition_Asset *asset = rendition->primary_asset_ = ads::new_object<Ads_Creative_Rendition_Asset>();
		asset->name_ = 	this->asset_.name_.c_str();
		asset->location_ = 	this->asset_.location_.c_str();
		asset->content_type_ = this->asset_.content_type_.c_str();
		asset->mime_type_ = this->asset_.mime_type_.c_str();
		asset->content_ = this->asset_.content_.c_str();

		if (!asset->content_type_.empty()) asset->content_type_id_ = repo->content_type(asset->content_type_.c_str());
		if (!this->asset_.bytes_.empty()) asset->bytes_ = ads::str_to_i64(this->asset_.bytes_.c_str());
	}

	for (std::list<Rendition_Asset *>::const_iterator it = this->other_assets_.begin(); it != this->other_assets_.end(); ++it)
	{
		const Rendition_Asset *ra = *it;

		Ads_Creative_Rendition_Asset *asset = ads::new_object<Ads_Creative_Rendition_Asset>();
		asset->name_ = 	ra->name_.c_str();
		asset->location_ = 	ra->location_.c_str();
		asset->content_type_ = ra->content_type_.c_str();
		asset->mime_type_ = ra->mime_type_.c_str();
		asset->content_ = ra->content_.c_str();

		if (!asset->content_type_.empty()) asset->content_type_id_ = repo->content_type(asset->content_type_.c_str());
		if (!ra->bytes_.empty()) asset->bytes_ = ads::str_to_i64(ra->bytes_.c_str());

		rendition->secondary_assets_.push_back(asset);
	}

	return rendition;
}

void
Ads_Selector::load_watched_rules(Ads_Selection_Context *ctx)
{
	Ads_String_List ids;
	ads::split(ctx->request_->o().watched_rules_, ids, ',');
	for (const auto& id : ids)
	{
		Ads_GUID rule_id = ads::entity::make_id(ADS_ENTITY_TYPE_MRM_RULE, ads::str_to_i64(id));
		try_add_watched_rule(ctx, rule_id, rule_id);
	}
}

bool
Ads_Selector::try_add_watched_rule(Ads_Selection_Context *ctx, Ads_GUID rule_id, Ads_GUID watched_id)
{
	const Ads_MRM_Rule *rule = nullptr;
	if (ctx->repository_->find_access_rule(rule_id, rule) < 0 || rule == nullptr)
	{
		ctx->log_watched_info(watched_id, "RULE_NOT_EXIST");
		ADS_DEBUG((LP_DEBUG, "watched rule %s doesn't exist\n", ADS_ENTITY_ID_CSTR(rule_id)));
		return false;
	}

	if (ctx->closures_.find(rule->network_id_) == ctx->closures_.end())
	{
		ctx->log_watched_info(watched_id, "RULE_OUT_OF_CLOSURE");
		ADS_DEBUG((LP_DEBUG, "watched rule %s belong to network %s and network isn't involved in this request\n",
		           ADS_ENTITY_ID_CSTR(rule_id),
		           ADS_ENTITY_ID_CSTR(rule->network_id_)));
		return false;
	}

	if (ctx->closures_.find(ads::entity::make_looped_id(rule->network_id_, true)) != ctx->closures_.end())
	{
		ctx->log_watched_info(watched_id, "LOOP_SCENARIO_NOTICE");
	}

	ctx->watched_rules_.insert(rule_id);
	return true;
}

void
Ads_Selector::load_watched_mkpl_orders(Ads_Selection_Context *ctx)
{
	Ads_String_List ids;
	ads::split(ctx->request_->o().watched_orders_, ids, ',');
	for (const Ads_String& id : ids)
	{
		Ads_GUID order_id = ads::entity::make_id(ADS_ENTITY_TYPE_MKPL_ORDER, ads::str_to_i64(id));
		try_add_watched_mkpl_order(ctx, order_id, order_id);
	}
}

bool
Ads_Selector::try_add_watched_mkpl_order(Ads_Selection_Context *ctx, Ads_GUID order_id, Ads_GUID watched_id)
{
	const Ads_MKPL_Order *order = nullptr;
	if (ctx->repository_->find_mkpl_order(order_id, order) < 0 || order == nullptr)
	{
		ctx->log_watched_info(watched_id, "ORDER_NOT_EXIST");
		ADS_DEBUG((LP_DEBUG, "watched order %s doesn't exist\n", ADS_ENTITY_ID_CSTR(order_id)));
		return false;
	}
	ctx->watched_orders_.insert(order_id);
	return true;
}

void
Ads_Selector::load_watched_deals(Ads_Selection_Context *ctx)
{
	Ads_String_List ids;
	ads::split(ctx->request_->o().watched_deals_, ids, ',');
	for (const Ads_String& id : ids)
	{
		Ads_GUID deal_id = ads::entity::make_id(ADS_ENTITY_TYPE_DEAL, ads::str_to_i64(id));
		try_add_watched_deal(ctx, deal_id);
	}
}

bool
Ads_Selector::try_add_watched_deal(Ads_Selection_Context *ctx, Ads_GUID deal_id)
{
	const Ads_Deal *deal = nullptr;
	if (ctx->repository_->find_deal(deal_id, deal) < 0 || deal == nullptr)
	{
		ctx->log_watched_info(deal_id, "DEAL_NOT_EXIST");
		ADS_DEBUG((LP_DEBUG, "watched deal %s doesn't exist\n", ADS_ENTITY_ID_CSTR(deal_id)));
		return false;
	}

	if (deal->rule_ != nullptr)
	{
		if (!try_add_watched_rule(ctx, deal->rule_id(), deal_id))
		{
			return false;
		}
	}
	else if (deal->mkpl_order_ != nullptr)
	{
		if (!try_add_watched_mkpl_order(ctx, deal->mkpl_order_->id_, deal_id))
		{
			return false;
		}
	}

	ctx->watched_deals_.insert(deal_id);
	return true;
}

void
Ads_Selector::load_watched_buyer_groups(Ads_Selection_Context *ctx)
{
	Ads_String_List ids;
	ads::split(ctx->request_->o().watched_buyer_groups_, ids, ',');
	for (const Ads_String& id : ids)
	{
		Ads_GUID buyer_group_id = ads::entity::make_id(ADS_ENTITY_TYPE_BUYER_GROUP, ads::str_to_i64(id));
		try_add_watched_buyer_group(ctx, buyer_group_id);
	}
}

bool
Ads_Selector::try_add_watched_buyer_group(Ads_Selection_Context *ctx, Ads_GUID buyer_group_id)
{
	const Ads_Buyer_Group *buyer_group = nullptr;
	if (ctx->repository_->find_buyer_group(buyer_group_id, buyer_group) < 0 || buyer_group == nullptr)
	{
		ctx->log_watched_info(buyer_group_id, "OPEN_EXCHANGE_NOT_EXIST");
		ADS_DEBUG((LP_DEBUG, "watched buyer group %s doesn't exist\n", ADS_ENTITY_ID_CSTR(buyer_group_id)));
		return false;
	}

	if (!try_add_watched_rule(ctx, buyer_group->rule_id(), buyer_group_id))
	{
		return false;
	}

	ctx->watched_buyer_groups_.insert(buyer_group_id);
	return true;
}

void
Ads_Selector::load_pg_override_creatives(Ads_Selection_Context *ctx)
{
	Ads_String_List res;
	int n = ads::split(ctx->request_->o().pg_creative_overrides_, res, ',');
	if (n < 2)
	{
		return;
	}

	Ads_String_List::iterator it = res.begin();
	Ads_GUID deal_id = ads::entity::make_id(ADS_ENTITY_TYPE_DEAL, ads::str_to_i64(*it));
	if (!try_add_watched_deal(ctx, deal_id))
	{
		return;
	}

	// parse market ad ids from res
	++it;
	std::vector<const Ads_Market_Ad*> market_ads;
	if (ctx->repository_->find_market_ads_of_deal(deal_id, market_ads) < 0)
	{
		return;
	}

	Ads_GUID_Set ingested_market_ad_ids, override_market_ad_ids;
	for (const auto* market_ad : market_ads)
	{
		ingested_market_ad_ids.insert(market_ad->id_);
	}

	for (; it != res.end(); ++it)
	{
		Ads_GUID market_ad_id = ads::str_to_i64(*it);
		if (ingested_market_ad_ids.find(market_ad_id) != ingested_market_ad_ids.end())
		{
			override_market_ad_ids.insert(market_ad_id);
		} else
		{
			Ads_String msg = "CREATIVE_NOT_FOUND " + ADS_ENTITY_ID_STR(market_ad_id);
			ctx->log_watched_info(deal_id, msg);
		}
	}
	if (!override_market_ad_ids.empty())
	{
		ctx->pg_creative_overrides_.emplace(deal_id, std::move(override_market_ad_ids));
	}
}

int
Ads_Selector::load_watched_candidate_advertisements(Ads_Selection_Context *ctx, Ads_GUID_Set *gads)
{
	Ads_String_List items;

	if (!gads)
	{
		ads::split(ctx->request_->o().allowed_ads_, items, ',');
		ADS_DEBUG((LP_TRACE, "load allowed ads: %s\n", ctx->request_->o().allowed_ads_.c_str()));
	}
	else
	{
		ads::split(ctx->request_->o().watched_ads_, items, ',');
//		ADS_DEBUG((LP_TRACE, "load watched ads: %s\n", ctx->request_->watched_ads_.c_str()));
	}

	for (Ads_String_List::const_iterator it = items.begin(); it != items.end(); ++it)
	{
		const Ads_String& s = *it;

		Ads_GUID_Set *ads = gads;
		const char *adid = s.c_str();
		if (!ads)
		{
			if (s[0] == '+')
				ads = &(ctx->allowed_candidates_);
			else if (s[0] == '-')
				ads = &(ctx->disallowed_candidates_);
			else if (s[0] == '*')
				ads = &(ctx->allowed_testing_candidates_);
			else if (Ads_Server_Config::instance()->enable_debug_)
			{
				if (s[0] == '$')
					ads = &(ctx->unlimited_budget_candidates_);
				else if (s[0] == '!')
					ads = &(ctx->uncompanion_candidates_);
			}

			if (ads) adid += 1;	//skip the prefix character

			// default to allowed ads if '+' is not specified
			if (!ads)
				ads = &(ctx->allowed_candidates_);
		}

		if (!ads)
		{
			ADS_DEBUG((LP_ERROR, "invalid item: %s\n", s.c_str()));
			continue;
		}

		Ads_GUID ad_id = ads::entity::make_id(ADS_ENTITY_TYPE_ADVERTISEMENT, ads::str_to_i64(adid));
		if (!ads::entity::is_valid_id(ad_id))
		{
			ADS_DEBUG((LP_ERROR, "invalid ad: %s\n", s.c_str()));
			continue;
		}

		Ads_GUID_List pending;
		pending.push_back(ad_id);

		while (!pending.empty())
		{
			Ads_GUID ad_id = pending.front();
			pending.pop_front();

			const Ads_Advertisement* ad = 0;
			if (ctx->find_advertisement(ad_id, ad) < 0 || !ad)
			{
				ADS_DEBUG((LP_ERROR, "ad %s not found\n", ADS_ENTITY_ID_CSTR(ad_id)));
				continue;
			}

			if (ad->type_ == Ads_Advertisement::AD_UNIT)
			{
				ads->insert(ad_id);

				// PUB-759
				// Before this fix, if user watch ads without watching corresponding placements, once placement
				// is rejected during selection, AdSever would not output diagnostic info for those watched ads
				// To fix this, watch placements for all watched ads so AdServer can output diagnostic info for
				// those watched ads when corresponding placement is rejected
				if (gads && ad->placement_) ads->insert(ad->placement_id());
			}
			else
			{
				//XXX: add placement id for watched ads
				if (ad->type_ == Ads_Advertisement::PLACEMENT && gads) ads->insert(ad_id);

				std::copy(ad->children_.begin(), ad->children_.end(), std::back_inserter(pending));
			}
		}
	}

	return 0;
}

int
Ads_Selector::load_explicit_candidate_advertisements(Ads_Selection_Context *ctx)
{
	ADS_ASSERT(ctx->request_->has_explicit_candidates());
	if (!ctx->request_->has_explicit_candidates()) return 0;

	bool check_targeting = ctx->request_->rep()->ads_.check_targeting_;

	typedef Ads_Request::Smart_Rep::Ad_Set Ad_Set;
	for (std::list<Ad_Set::Ad *>::const_iterator ait = ctx->request_->rep()->ads_.ads_.begin();
	        ait != ctx->request_->rep()->ads_.ads_.end();
	        ++ait
	    )
	{
		Ad_Set::Ad *ad_ref = *ait;
		ADS_ASSERT(ad_ref);

		Ads_GUID ad_id = ads::entity::invalid_id(ADS_ENTITY_TYPE_ADVERTISEMENT);
		if (!ad_ref->id_.empty())
		{
			if (ad_ref->id_[0] == 's')
				ad_id = ads::entity::make_id(ADS_ENTITY_TYPE_SYSTEM_ADVERTISEMENT, ads::str_to_i64(ad_ref->id_.substr(1).c_str()));
			else
				ad_id = ads::ad::make_id(0, ads::str_to_i64(ad_ref->id_.c_str()));
		}

		if (!ads::entity::is_valid_id(ad_id)) continue;

		Ads_GUID rendition_id = -1;
		if (ad_ref->creative_)
		{
			int64_t n = ads::str_to_i64(ad_ref->creative_->id_.c_str());
			if (n > 0) rendition_id = ads::entity::make_id(ad_ref->creative_->is_creative_? ADS_ENTITY_TYPE_CREATIVE: ADS_ENTITY_TYPE_CREATIVE_RENDITION, n);
		}

		Ads_GUID_List pending;
		pending.push_back(ad_id);
#ifdef ADS_ENABLE_FORECAST
		if (!ads::entity::is_looped_id(ad_id))
		{
			const Ads_Advertisement_Candidate_Lite* looped_ad_lite = ctx->find_advertisement_candidate_lite(ads::entity::make_looped_id(ad_id, true), nullptr);
			if (looped_ad_lite)
			{
				pending.push_back(ads::entity::make_looped_id(ad_id, true));
			}
		}
#endif

		size_t count =  0;
		const size_t MAX_ITERATION = 10000;
		while (!pending.empty() && ++count < MAX_ITERATION)
		{
			Ads_GUID ad_id = pending.front();
			pending.pop_front();

			Ads_Advertisement_Candidate_Lite* ad_lite = ctx->find_advertisement_candidate_lite(ad_id, nullptr, &selection::Dummy_Ad_Owner::instance());
			if (!ad_lite)
			{
				ctx->error(ADS_ERROR_ADVERTISEMENT_NOT_FOUND, ads::Error_Info::SEVERITY_WARN, ADS_ERROR_ADVERTISEMENT_NOT_FOUND_S, "", ADS_ENTITY_ID_STR(ad_id));
				continue;
			}

			if (!ad_lite->is_active(ctx->time(), ctx->enable_testing_placement()) && !ad_lite->has_tracking_creative())
			{
				ctx->error(ADS_ERROR_ADVERTISEMENT_INACTIVE, ads::Error_Info::SEVERITY_WARN, ADS_ERROR_ADVERTISEMENT_INACTIVE_S, "", ADS_ENTITY_ID_STR(ad_id));
				continue;
			}

			if (this->is_advertisement_in_blacklist(ctx, ad_lite->ad())
//#if defined(ADS_ENABLE_FORECAST)
				|| (!ad_lite->is_sponsorship()
					&& !ad_lite->test_advertisement_blacklist(&ctx->restriction_.advertiser_blacklist_, &ctx->restriction_.advertiser_whitelist_, &ctx->restriction_.industry_blacklist_, &ctx->restriction_.reseller_blacklist_, &ctx->restriction_.reseller_whitelist_, &ctx->restriction_.targeting_inventory_blacklist_, &ctx->restriction_.targeting_inventory_whitelist_, &ctx->restriction_.brand_blacklist_, &ctx->restriction_.brand_whitelist_,
							&ctx->restriction_.global_brand_blacklist_, &ctx->restriction_.global_brand_whitelist_, &ctx->restriction_.global_advertiser_blacklist_, &ctx->restriction_.global_advertiser_whitelist_,
							ctx->restriction_))
//#endif
                )
			{
				ctx->error(ADS_ERROR_ADVERTISEMENT_BLOCKED, ads::Error_Info::SEVERITY_WARN, ADS_ERROR_ADVERTISEMENT_BLOCKED_S, "", ADS_ENTITY_ID_STR(ad_id));
				continue;
			}

			if (check_targeting &&
			    (ad_lite->type() == Ads_Advertisement::PLACEMENT ||
			     ad_lite->type() == Ads_Advertisement::AD_UNIT))
			{
#ifdef ADS_ENABLE_FORECAST
				if (ad_lite->ad_ != nullptr && ad_lite->ad_->is_scheduled_only())
					continue;
				//INK-1801: check comscore index
				if (!this->test_comscore_targeting(ctx, ad_lite->ad())) // for ad unit and placement
					continue;
				if (ad_lite->type() == Ads_Advertisement::AD_UNIT) //ad unit needs to take care of its parents...
				{
					const Ads_Advertisement_Candidate_Lite* parent_lite = ad_lite->parent();
					if (!parent_lite || !this->test_comscore_targeting(ctx, parent_lite->ad()))
					{
						continue;
					}
				}
#endif

				const auto *closure = ad_lite->owner_.to_closure();
				if (!closure)
				{
					ctx->error(ADS_ERROR_NETWORK_NOT_RESELLABLE, ads::Error_Info::SEVERITY_WARN, ADS_ERROR_NETWORK_NOT_RESELLABLE_S, "", ADS_ENTITY_ID_CSTR(ad_lite->network_id()));

					ADS_DEBUG((LP_TRACE, "network %s for ad %s is not connected\n", ADS_ENTITY_ID_CSTR(ad_lite->network_id()), ADS_ENTITY_ID_CSTR(ad_lite->id())));
					continue;
				}

				// test targeting
				ADS_ASSERT(closure);

				const Ads_GUID_Set& terms = closure->terms_;

				//if (!this->test_advertisement_targeting(ctx, closure, conjunctions, ad)) continue;
				if (!this->test_targeting_criteria(ctx->repository_, ctx->delta_repository_, terms, ad_lite->targeting_criteria()))
					continue;

				///CAUTION: ad unit needs to take care of its parents...
				if (ad_lite->type() == Ads_Advertisement::AD_UNIT)
				{
					const Ads_Advertisement_Candidate_Lite* parent_ad_lite = ad_lite->parent();
					if (!parent_ad_lite
						 //|| !this->test_advertisement_targeting(ctx, closure, conjunctions, parent)))
						 || !this->test_targeting_criteria(ctx->repository_, ctx->delta_repository_, terms, parent_ad_lite->targeting_criteria()))
					{
						continue;
					}
				}
			}

			if (ad_lite->type() == Ads_Advertisement::AD_UNIT)
			{
				if (ads::entity::is_valid_id(rendition_id))
				{
					const Ads_Creative_Rendition *rendition = 0;
					ctx->find_creative_rendition(rendition_id, rendition);
//					ctx->explicit_creatives_.insert(std::make_pair(ad_id, rendition_id));

					if (!rendition)
					{
						ctx->explicit_renditions_.insert(std::make_pair(ad_id, rendition));
						ctx->error(ADS_ERROR_CREATIVE_RENDITION_NOT_FOUND, ads::Error_Info::SEVERITY_WARN, ADS_ERROR_CREATIVE_RENDITION_NOT_FOUND_S, "", ADS_ENTITY_ID_STR(rendition_id));
					}
					else
					{
						if (ads::entity::type(rendition_id) == ADS_ENTITY_TYPE_CREATIVE_RENDITION)
							ctx->explicit_renditions_.insert(std::make_pair(ad_id, rendition));
						else
						{
							for (Ads_GUID_RSet::const_iterator it = rendition->children_.begin(); it != rendition->children_.end(); ++it)
							{
								const Ads_Creative_Rendition *r = 0;
								ctx->find_creative_rendition(*it, r);
								if (r) ctx->explicit_renditions_.insert(std::make_pair(ad_id, r));
							}
						}
					}
				}
				else if (ad_ref->creative_)
				{
					Ads_Creative_Rendition *rendition = ad_ref->creative_->to_entity(ctx->repository_);
//					ctx->custom_creatives_.insert(std::make_pair(ad_id, creative));

					ctx->explicit_renditions_.insert(std::make_pair(ad_id, rendition));
					ctx->renditions_.push_back(rendition);
				}

				if (ad_lite->is_bumper())
					try_merge_bumper(ctx, ad_lite);
				else
					ctx->flattened_candidates_.insert(ad_lite);
			}
			else
			{
#ifdef ADS_ENABLE_FORECAST
				//hylda cross request exclusivity
				if (ad_lite->type() == Ads_Advertisement::PLACEMENT && ctx->request_->rep()->capabilities_.synchronize_multiple_requests_)
				{
					if (!ctx->scheduler_->airing_cross_break_advertisements_.empty())
					{
						if (!ad_lite->test_advertisement_exclusivity(ctx->scheduler_->airing_cross_break_advertisements_, false, false, true, true))
						{
							ADS_DEBUG((LP_TRACE, "%s failed (HYLDA)\n",  ADS_ADVERTISEMENT_ID_CSTR(ad_id)));
							continue;
						}
					}
				}
				bool is_loop = ads::entity::is_looped_id(ad_id);
				for (Ads_GUID_RSet::const_iterator it = ad_lite->children().begin(); it != ad_lite->children().end(); ++it)
					pending.push_back(ads::entity::make_looped_id(*it, is_loop));
#else
				std::copy(ad_lite->children().begin(), ad_lite->children().end(), std::back_inserter(pending));
#endif
			}
		}
	}

	return 0;
}

int
Ads_Selector::transfer_selected_candidate_advertisements(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate * candidate, std::list<Ads_Advertisement_Candidate_Ref *>& candidate_refs,
		Take_Advertisement_Slot_Position* slot_position, Take_Advertisement_Candidate_Rank* candidate_rank)
{
	// transfer to candidate reference
	if (candidate->companion_candidates_.empty()
		|| candidate->flags_ & Ads_Advertisement_Candidate::FLAG_NO_TEMPORAL
		|| candidate->ad_unit()->is_temporal())
	{
		Ads_Advertisement_Candidate_Vector companion_candidates;

		if (candidate->ad_unit()->is_temporal())
			for (Ads_Advertisement_Candidate_Vector::iterator ait = candidate->companion_candidates_.begin();
				 ait != candidate->companion_candidates_.end(); ++ait)
			{
				Ads_Advertisement_Candidate* current = *ait;
				if (!current->ad_unit()->is_temporal())
					companion_candidates.push_back(current);
			}

			Ads_Advertisement_Candidate *current = candidate;
			if (!current->slot_) return -1;

			Ads_Advertisement_Candidate_Ref *ref = new Ads_Advertisement_Candidate_Ref(current, current->slot_);
			ref->replica_id_ = current->references_.size();

			current->used_slots_.insert(current->slot_);
			current->references_.push_back(ref);

			this->calculate_effective_inventory(ctx, ref);
#if defined(ADS_ENABLE_FORECAST)
			this->take_advertisement_candidate_reference(ctx, ref, candidate_refs, slot_position, candidate_rank);
#else
			this->take_advertisement_candidate_reference(ctx, ref, candidate_refs);
#endif

			//FDB-4133 no companion display for follower ads
			if (current->ad_->is_follower())
				return -1;

			for (Ads_Advertisement_Candidate_Vector::iterator cit = companion_candidates.begin();
				 cit != companion_candidates.end(); ++cit)
			{
				Ads_Advertisement_Candidate *companion = *cit;
				if (!companion->slot_) continue;

				companion->used_slots_.insert(companion->slot_);

				Ads_Advertisement_Candidate_Ref *aref = new Ads_Advertisement_Candidate_Ref(companion, companion->slot_);
				aref->replica_id_ = companion->references_.size();
				aref->is_companion_ = true;

				this->calculate_effective_inventory(ctx, aref);
				companion->references_.push_back(aref);

				ref->companion_candidates_.push_back(aref);
				aref->companion_candidates_.push_back(ref);

#if defined(ADS_ENABLE_FORECAST)
				this->take_advertisement_candidate_reference(ctx, aref, candidate_refs, slot_position, candidate_rank);
#else
				this->take_advertisement_candidate_reference(ctx, aref, candidate_refs);
#endif
			}

		candidate->slot_ = 0;
	}

	return 0;
}

int
Ads_Selector::try_replicate_selected_candidate_advertisements(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate_List& candidates, std::list<Ads_Advertisement_Candidate_Ref *>& candidate_refs,
		Take_Advertisement_Slot_Position* slot_position, Take_Advertisement_Candidate_Rank* candidate_rank)
{
	int64_t profit = 0;

	if (ctx->root_asset_)
	{
#if defined(ADS_ENABLE_FORECAST)
		this->try_replicate_selected_candidate_advertisements(ctx, candidates, ctx->video_slots_, candidate_refs, profit, slot_position, candidate_rank);
		this->try_replicate_selected_candidate_advertisements(ctx, candidates, ctx->player_slots_, candidate_refs, profit, slot_position, candidate_rank);
#else
		this->try_replicate_selected_candidate_advertisements(ctx, candidates, ctx->video_slots_, candidate_refs, profit);
		this->try_replicate_selected_candidate_advertisements(ctx, candidates, ctx->player_slots_, candidate_refs, profit);
#endif
	}

	if (ctx->root_section_)
#if defined(ADS_ENABLE_FORECAST)
		this->try_replicate_selected_candidate_advertisements(ctx, candidates, ctx->page_slots_, candidate_refs, profit, slot_position, candidate_rank);
#else
		this->try_replicate_selected_candidate_advertisements(ctx, candidates, ctx->page_slots_, candidate_refs, profit);
#endif

	return 0;
}

int
Ads_Selector::try_replicate_candidate_advertisement(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate *candidate,
		Ads_Slot_Base *slot, std::list<Ads_Advertisement_Candidate_Ref *>& new_refs, int64_t& profit, bool repeat_within_slot)
{
	bool recoverable = false;
	return this->try_replicate_candidate_advertisement(ctx, candidate, slot, new_refs, profit, repeat_within_slot, recoverable);
}

int
Ads_Selector::try_replicate_candidate_advertisement(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate *candidate, Ads_Slot_Base *slot, std::list<Ads_Advertisement_Candidate_Ref *>& new_refs, int64_t& profit, bool repeat_within_slot, bool& recoverable)
{
	//market ad translated in phase 8 SSTF
	if (candidate->is_pre_selection_external_translated()) return 0;

	/// tracking ad
	if (candidate->ad_->is_tracking()) return 0;

	// slot is used
	if (ctx->is_slot_used(slot)) return 0;

	/// no space
	if (slot->env() == ads::ENV_VIDEO)
	{
		Ads_Video_Slot *vslot = reinterpret_cast<Ads_Video_Slot *>(slot);
		if (vslot == nullptr)
			return -1;
		if (vslot->is_full())
			return -1;
		if (!vslot->has_available_space(candidate->duration(vslot)))
			return 0;
	}

	/// slot used by candidate
	if (!repeat_within_slot && candidate->used_slots_.find(slot) != candidate->used_slots_.end())
		return 1;

	//scte-130 no repeat in group
	if (slot->env() == ads::ENV_VIDEO)
	{
		Ads_Video_Slot *vslot = reinterpret_cast<Ads_Video_Slot *>(slot);
		if (vslot->pod_group_id_ != -1)
		{
			bool occupied = false;
			for (Ads_Slot_Set::const_iterator uit = candidate->used_slots_.begin(); uit != candidate->used_slots_.end(); ++uit)
			{
				if ((*uit)->env() == ads::ENV_VIDEO
						&& reinterpret_cast<Ads_Video_Slot *>(*uit)->pod_group_id_ == vslot->pod_group_id_)
				{
					occupied = true;
					break;
				}
			}

			if (!repeat_within_slot && occupied && !candidate->ad_->is_portfolio())
			{
				ADS_DEBUG((LP_DEBUG, "scte ad %ld failed to repeat in slot %s (has already filled into group %d)\n",
							CANDIDATE_LITE_ID_CSTR(candidate->lite_),
							vslot->custom_id_.c_str(),
							vslot->pod_group_id_));
				return 1;
			}
		}
	}

	/// FDB-2998 work around, prevent 2 copies in one parent slot.
	if (!repeat_within_slot && candidate->is_parent_slot_used(slot)
#if defined (ADS_ENABLE_FORECAST)
			// work around over the work around: allow repeat in parent slot for ugp/puga/ta
			&& (!(ctx->request_flags() & Ads_Request::Smart_Rep::BYPASS_BUDGET_CHECK)
				|| (ctx->scheduler_->is_digital_live_schedule() && !candidate->ad_->is_portfolio()))
#endif
	   ) return 1;

	if (!ctx->fc_->pass_frequency_cap_by_slot(ctx, candidate))
		return 0;

	if (!ctx->fc_->pass_frequency_cap_time_based(ctx, candidate, slot))
		return 0;

	bool verbose = (ctx->verbose_ > 0);
	if (!this->test_advertisement_slot_restrictions(ctx, candidate, slot, 0, verbose, recoverable))
	{
		return 0;
	}

	if (!this->test_advertisement_unit_sponsorship(candidate->lite_, slot, verbose))
		return 0;
	if (!candidate->test_advertisement_exclusivity(ctx, ctx->selected_candidates_, slot))
		return 0;

	if (candidate->ad_->is_follower())
	{
		const Ads_Advertisement_Candidate* leader = 0;

		if (this->find_leader_for_follower(ctx, candidate, leader, ctx->video_slots_, slot) < 0 || !leader)
		{
			ADS_DEBUG((LP_DEBUG, "no leader found for follower %s, slot %s\n", CANDIDATE_LITE_ID_CSTR(candidate->lite_), slot->custom_id_.c_str()));
			if (candidate->lite_->is_watched()) ctx->log_watched_info(*candidate->lite_, slot, "NO_LEADER_FOUND");
			return 0;
		}
	}
	bool force_repeated = false;
#if defined (ADS_ENABLE_FORECAST)
	if (candidate->ad_unit()->type_ == Ads_Slot_Restriction::SYSTEM_DEFAULT && candidate->ad_unit()->slot_type_ == Ads_Ad_Unit::DISPLAY && !slot->accept_standalone())
		return 0;
	if (candidate->ad_->repeat_mode_ == Ads_Advertisement::DO_NOT_REPEAT && !(ctx->request_flags() & Ads_Request::Smart_Rep::BYPASS_RESTRICTION)) force_repeated = true;
	if (candidate->ad_unit()->take_all_chances_) force_repeated = false;
#endif

	if (candidate->is_companion())
	{
		bool is_packable = (slot->env() == ads::ENV_VIDEO);
		bool check_initial = false;
		bool has_temporal = false, select_temporal = false;

		Ads_Advertisement_Candidate *driven_candidate = 0;

		if (slot->env() == ads::ENV_PAGE || slot->env() == ads::ENV_PLAYER) check_initial = true;
		if (slot->env() == ads::ENV_VIDEO)
		{
			select_temporal = true;
			driven_candidate = candidate;

			if (ctx->request_->rep()->site_section_.video_player_.video_.auto_play_
					&& !ctx->keep_initial_ad()
					&& !slot->force_first_call()
					&& int64_t(reinterpret_cast<Ads_Video_Slot *>(slot)->time_position_) <= ctx->max_initial_time_position_)
				check_initial = true;
		}

		Ads_Advertisement_Candidate_Vector companion_candidates;
		for (size_t i = 0; i < candidate->companion_candidates_.size(); ++i)
		{
			Ads_Advertisement_Candidate *current = candidate->companion_candidates_[i];
			bool is_temporal = current->ad_unit() && current->ad_unit()->is_temporal();

			if (is_temporal)
			{
				if (driven_candidate && current != driven_candidate)
					continue;

				driven_candidate = current;
				has_temporal = true;
			}

			companion_candidates.push_back(current);
		}

		if (!has_temporal)
		{
			/// DISPLAY companion only repeat itself
			companion_candidates.clear();
			companion_candidates.push_back(candidate);
		}
		else if (slot->env() == ads::ENV_PAGE || slot->env() == ads::ENV_PLAYER)
			/// DISPLAY continue if companion has temporal
			return 0;

		size_t n_companions = companion_candidates.size();
		size_t flags = 0;

		std::vector<Ads_Slot_Base *> plan(n_companions);
		std::multimap<Ads_Slot_Base *, Ads_Advertisement_Candidate *> occupied;

		bool satisfied = true;

		for (size_t i = 0; i < n_companions; ++i)
			plan[i] = 0;

		occupied.insert(std::make_pair(slot, candidate));
		for (size_t i = 0; i < n_companions; ++i)
		{
			Ads_Advertisement_Candidate *current = companion_candidates[i];

			if (current == candidate)
			{
				plan[i] = slot;
				continue;
			}

			//FDB-4133 no companion display for follower ads
			if (candidate->ad_->is_follower())
				continue;

#if 0
			/// companion display
			for (Ads_Slot_List::iterator it = ctx->player_slots_.begin(); it != ctx->player_slots_.end(); ++it)
			{
				Ads_Slot_Base *bslot = *it;

				if (!bslot->accept_companion() &&
						!(current->flags_ & Ads_Advertisement_Candidate::FLAG_NO_TEMPORAL)
				   )
				{
					if (current->is_watched()) ctx->log_watch(current->ad_->id_, slot, "NOT_ACCEPT_COMPANION");
					continue;
				}
				if (check_initial &&
						(!bslot->accept_initial() || ctx->is_slot_used(bslot, Ads_Selection_Context::STANDALONE))
				   )
					continue;

				/// slot used
				if (!is_packable)
				{
					if (ctx->is_slot_used(bslot)) continue;
					if (current->used_slots_.find(bslot) != current->used_slots_.end()) continue;
				}

				/// slot occupied
				if (occupied.find(bslot) != occupied.end()) continue;

				if (this->test_advertisement_slot_restrictions(ctx, current, bslot, 0, verbose))
				{
					plan[i] = bslot;
					occupied.insert(std::make_pair(bslot, current));
				}

				if (plan[i]) break;
			}

			if (plan[i]) continue;

			for (Ads_Slot_List::iterator it = ctx->page_slots_.begin(); it != ctx->page_slots_.end(); ++it)
			{
				Ads_Slot_Base *bslot = *it;

				if (!bslot->accept_companion() &&
						!(current->flags_ & Ads_Advertisement_Candidate::FLAG_NO_TEMPORAL)
				   )
				{
					if (current->is_watched()) ctx->log_watch(current->ad_->id_, slot, "NOT_ACCEPT_COMPANION");
					continue;
				}
				if (check_initial &&
						(!bslot->accept_initial() || ctx->is_slot_used(bslot, Ads_Selection_Context::STANDALONE)))
					continue;

				/// slot used
				if (!is_packable)
				{
					if (ctx->is_slot_used(bslot)) continue;
					if (current->used_slots_.find(bslot) != current->used_slots_.end()) continue;
				}

				/// slot occupied
				if (occupied.find(bslot) != occupied.end()) continue;

				if (this->test_advertisement_slot_restrictions(ctx, current, bslot, 0, verbose))
				{
					plan[i] = bslot;
					occupied.insert(std::make_pair(bslot, current));
				}

				if (plan[i]) break;
			}
#else
			plan[i] = find_companion_display_slot(ctx, ctx->player_slots_, current, check_initial, is_packable, occupied);
			if (!plan[i])
				plan[i] = find_companion_display_slot(ctx, ctx->page_slots_, current, check_initial, is_packable, occupied);
#endif
			if (plan[i]) continue;
		}

		if (has_temporal && !select_temporal) satisfied = false;

		if (!satisfied)
		{
			ADS_DEBUG((LP_DEBUG, "companion package %s not able to deliver\n", CANDIDATE_LITE_ID_CSTR(candidate->lite_)));
			return 0;
		}

		Ads_Advertisement_Candidate_Set dummy;
		this->take_companion(ctx, candidate, companion_candidates, dummy, plan, flags, profit);

		std::vector<Ads_Advertisement_Candidate_Ref *> companion_refs;

		for (size_t i = 0; i < n_companions; ++i)
		{
			Ads_Slot_Base *aslot = plan[i];
			if (!aslot) continue;

			Ads_Advertisement_Candidate *current = companion_candidates[i];

			Ads_Advertisement_Candidate_Ref *ref = new Ads_Advertisement_Candidate_Ref(current, aslot);
			ref->replica_id_ = current->references_.size();
			ref->is_companion_ = (has_temporal && current->ad_unit() && !current->ad_unit()->is_temporal());

			if (force_repeated) ref->flags_ |= Ads_Advertisement_Candidate_Ref::FLAG_FORCE_REPEATED; // only for forecast

			current->used_slots_.insert(ref->slot_);
			current->references_.push_back(ref);
			this->calculate_effective_inventory(ctx, ref);

			companion_refs.push_back(ref);
			new_refs.push_back(ref);
			if (current->is_multiple_ads_)
			{
				slot->filled_by_multi_ads_ = true;
				current->replacable(slot, false);
			}
		}

		for (std::vector<Ads_Advertisement_Candidate_Ref *>::iterator it = companion_refs.begin();
				it != companion_refs.end();
				++it)
		{
			Ads_Advertisement_Candidate_Ref *ref = *it;
			ref->companion_candidates_ = companion_refs;
		}
	}
	else
	{
		Ads_Advertisement_Candidate_Ref *ref = new Ads_Advertisement_Candidate_Ref(candidate, slot);
		ref->replica_id_ = candidate->references_.size();
		if (force_repeated) ref->flags_ |= Ads_Advertisement_Candidate_Ref::FLAG_FORCE_REPEATED; // only for forecast

		candidate->used_slots_.insert(ref->slot_);
		candidate->references_.push_back(ref);

		this->calculate_effective_inventory(ctx, ref);

		new_refs.push_back(ref);
		if (candidate->is_multiple_ads_)
		{
			slot->filled_by_multi_ads_ = true;
			candidate->replacable(slot, false);
		}

		/// frequency capping
		ctx->fc_->inc_occurences(candidate, slot, ctx);

		profit += candidate->effective_eCPM(slot->associate_type());

		if (slot->env() == ads::ENV_PLAYER || slot->env() == ads::ENV_PAGE)
		{
			{
				ref->slot_->load_order_ = "i";
				ctx->use_slot(candidate, ref->slot_, Ads_Selection_Context::STANDALONE);
			}
		}
		else if (slot->env() == ads::ENV_VIDEO)
		{
			reinterpret_cast<Ads_Video_Slot*>(slot)->take_advertisement(ctx, candidate);
		}
	}
	return 1;
}

Ads_Slot_Base* Ads_Selector::find_companion_display_slot(Ads_Selection_Context *ctx, Ads_Slot_List& slot_candidates, Ads_Advertisement_Candidate* ad_candidate, bool check_initial, bool is_packable,
		std::multimap<Ads_Slot_Base *, Ads_Advertisement_Candidate *> &occupied)
{
	bool verbose = (ctx->verbose_ > 0);
	Ads_Slot_Base* slot_result = 0;
	/// companion display
	for (Ads_Slot_List::iterator it = slot_candidates.begin(); it != slot_candidates.end(); ++it)
	{
		Ads_Slot_Base *bslot = *it;

		if (!bslot->accept_companion() &&
			!(ad_candidate->flags_ & Ads_Advertisement_Candidate::FLAG_NO_TEMPORAL)
		   )
		{
			if (ad_candidate->lite_->is_watched()) ctx->log_watched_info(*ad_candidate->lite_, bslot, "NOT_ACCEPT_COMPANION");
			continue;
		}
		if (check_initial &&
			(!bslot->accept_initial() || ctx->is_slot_used(bslot, Ads_Selection_Context::STANDALONE))
		   )
			continue;

		/// slot used
		if (!is_packable)
		{
			if (ctx->is_slot_used(bslot)) continue;
			if (ad_candidate->used_slots_.find(bslot) != ad_candidate->used_slots_.end()) continue;
		}

		/// slot occupied
		if (occupied.find(bslot) != occupied.end()) continue;

		if (this->test_advertisement_slot_restrictions(ctx, ad_candidate, bslot, 0, verbose))
		{
			slot_result = bslot;
			occupied.insert(std::make_pair(bslot, ad_candidate));
		}

		if (slot_result) return slot_result;
	}
	return 0;
}


int
Ads_Selector::load_scheduled_ad_display_companion_plan(Ads_Selection_Context* ctx,
	bool check_initial,
	Ads_Slot_List& player_slots,
	Ads_Slot_List& page_slots,
	const Ads_Advertisement_Candidate_Vector& companion_candidates,
	std::map<Ads_Advertisement_Candidate*, Ads_Slot_Base*>& avaliable_display_candidate_slot)
{
	std::multimap<Ads_Slot_Base*, Ads_Advertisement_Candidate*> display_candidate_slot_occupied;
	for (auto* companion_candidate : companion_candidates)
	{
		if (companion_candidate == nullptr)
		{
			ADS_LOG((LP_ERROR, "companion_candidate nullptr meet\n"));
			continue;
		}
		if (companion_candidate->ad_ == nullptr || companion_candidate->ad_unit() == nullptr)
		{
			ADS_LOG((LP_ERROR, "companion_candidate nullptr meet[%p]\n", companion_candidate->ad_));
			continue;
		}
		if (companion_candidate->ad_unit()->is_temporal())
		{
			continue;
		}
		Ads_Advertisement_Candidate* display_candidate = companion_candidate;
		bool is_packable = true;
		//first find slot in the player slots
		Ads_Slot_Base* available_display_slot = find_companion_display_slot(ctx, player_slots, display_candidate,
			check_initial, is_packable, display_candidate_slot_occupied);
		if (available_display_slot == nullptr)
		{
			ADS_DEBUG((LP_DEBUG, "ad %s can not find available player slot\n", ADS_ENTITY_ID_CSTR(display_candidate->ad_->id_)));
			//then find slot in the page slots
			available_display_slot = find_companion_display_slot(ctx, page_slots, display_candidate,
				check_initial, is_packable, display_candidate_slot_occupied);
		}
		if (available_display_slot != nullptr)
		{
			avaliable_display_candidate_slot[display_candidate] = available_display_slot;
		}
		else
		{
			ADS_DEBUG((LP_DEBUG, "ad %s can not find available page slot\n", ADS_ENTITY_ID_CSTR(display_candidate->ad_->id_)));
		}
	}
	return avaliable_display_candidate_slot.empty() ? -1 : 0;
}

/***********************************
 *	@description: get_custom_set_link_info, need to save the link_type and other info into the link_info_list, because the custom set in placemnet do not contain this infomation
 *	@params:
 *			ctx,
 *			placement, the placement of the custom set
 *			scheduled_ad,
 *			link_info_map(out)
 * ********************************/
int
Ads_Selector::get_custom_set_link_info(Ads_Selection_Context* ctx,
	const Ads_Advertisement& placement,
	const Ads_Advertisement& scheduled_ad,
	std::map<Ads_GUID, Custom_Linking_Set>& link_info_map)
{
	//use the orignal palcement.childern to evaluate the link info
	for (const auto child_ad_id : placement.children_)
	{
		const Ads_Advertisement* child_ad = nullptr;
		if (ctx->find_advertisement(child_ad_id, child_ad) < 0)
		{
			ADS_LOG((LP_ERROR, "ad(%s) find error in repa\n", ADS_ENTITY_ID_CSTR(child_ad_id)));
			continue;
		}
		if (child_ad == nullptr)
		{
			ADS_LOG((LP_ERROR, "ad(%s) can not be found in repa\n", ADS_ENTITY_ID_CSTR(child_ad_id)));
			continue;
		}
		if (!child_ad->is_active(ctx->time(), ctx->enable_testing_placement()))
		{
			ADS_DEBUG((LP_DEBUG, "ad(%s) is inactive\n", ADS_ENTITY_ID_CSTR(child_ad_id)));
			continue;
		}
		if (!child_ad->companion_)
		{
			ADS_DEBUG((LP_DEBUG, "ad(%s) is not companion\n", ADS_ENTITY_ID_CSTR(child_ad_id)));
			continue;
		}
		if (child_ad->ad_unit() == nullptr)
		{
			ADS_LOG((LP_ERROR, "ad(%s)'s ad_unit is nullptr\n", ADS_ENTITY_ID_CSTR(child_ad_id)));
			continue;
		}
		for (auto& link_group_pair : child_ad->ad_linking_groups_)
		{
			Ads_GUID custom_set_id = link_group_pair.first;
			auto& link_info = link_info_map[custom_set_id];
			link_info.size_++;//caculate the total ads count of the custom set
			link_info.mode_ = link_group_pair.second;//record the link type
			if (child_ad == &scheduled_ad)
			{
				link_info.contains_scheduled_ad_ = true;
			}
			if (child_ad->ad_unit()->is_temporal())
			{
				link_info.only_contains_display_ad_ = false;
			}
		}
	}
	return 0;
}
/*******************************************
 *	@description: convert avaliable_display_candidate_slot to companion_plan (for take companion) and to display_candidate_slot_occupied (for frequency_cap)
 *	@params:
 *			scheduled_candidate,
 *			slot_for_scheduled_candidate, the slot which scheduled ad will fill in
 *			avaliable_display_candidate_slot, companion ad which filled in slot
 *			companion_plan(out)
 *			display_candidate_slot_occupied(out)
 *	@return: 0 (always)
 * **********************************/

int
Ads_Selector::convert_validate_companion_slot_for_plan(
		const Ads_Advertisement_Candidate& scheduled_candidate,
		const Ads_Video_Slot& slot_for_scheduled_candidate,
		const std::map<Ads_Advertisement_Candidate*, Ads_Slot_Base*>& avaliable_display_candidate_slot,
		std::vector<Ads_Slot_Base *>& companion_plan,
		std::multimap<Ads_Slot_Base*, Ads_Advertisement_Candidate*>& display_candidate_slot_occupied)
{
	size_t companion_size = scheduled_candidate.companion_candidates_.size();
	companion_plan.resize(companion_size);
	//conver the avaliable_display_candidate_slot to plan
	for (size_t i = 0; i< companion_size; ++i)
	{
		companion_plan[i] = nullptr;
		auto* companion_candidate = scheduled_candidate.companion_candidates_[i];
		if (companion_candidate == nullptr)
		{
			ADS_LOG((LP_ERROR, "companion_candidate is null\n"));
			continue;
		}
		if (companion_candidate == &scheduled_candidate)
		{
			auto* p_slot_for_scheduled_candidate_no_const = const_cast<Ads_Video_Slot*>(&slot_for_scheduled_candidate);
			companion_plan[i] = p_slot_for_scheduled_candidate_no_const;
			continue;
		}
		const auto avaliable_display_candidate_slot_it = avaliable_display_candidate_slot.find(companion_candidate);
		if (avaliable_display_candidate_slot_it == avaliable_display_candidate_slot.end())
		{
			continue;
		}
		auto* slot_for_companion = avaliable_display_candidate_slot_it->second;
		companion_plan[i] = slot_for_companion;
		display_candidate_slot_occupied.insert(std::make_pair(slot_for_companion, companion_candidate));
	}
	return 0;
}


/*************************
 *	description: get_very_advanced_companion_candidate_for_validation
 *				get the validated slot and candidate from the companion candidate
 *	params:
 *			link_set_list all custom_set of the placement
 *			link_info_map, all custom_set link mode info
 *			all_companion_candidate_for_validation(output), function push the valide custome set's ad into it
 *	return : 0 (always)
 * ****/
int
Ads_Selector::get_very_advanced_companion_candidate_for_validation(
	const Linking_Set_List_Vector& link_set_list,
	const std::map<Ads_GUID, Custom_Linking_Set>& link_info_map,
	Ads_Advertisement_Candidate_Vector& all_companion_candidate_for_validation)
{
	for (auto& link_set_pair : link_set_list)
	{
		Ads_GUID custom_set_id = link_set_pair.first;
		const Ads_Advertisement_Candidate_List& ads_in_custom_set = link_set_pair.second;
		auto custom_set_info_pair_it = link_info_map.find(custom_set_id);
		if (custom_set_info_pair_it == link_info_map.end())
		{
			ADS_LOG((LP_ERROR, "can not find the custom set in link_info_map\n"));
			continue;
		}
		auto& custom_set_info = (*custom_set_info_pair_it).second;
		size_t origin_ads_count_in_custom_set = custom_set_info.size_;
		auto companion_mode = custom_set_info.mode_;
		bool only_contains_display_ad = custom_set_info.only_contains_display_ad_;
		bool contains_scheduled_ad = custom_set_info.contains_scheduled_ad_;


		ADS_DEBUG((LP_DEBUG, "custom set(%s) is being checked , size count(%d), origin_ads_count_in_custom_set(%d)\n", ADS_ENTITY_ID_CSTR(custom_set_id), ads_in_custom_set.size(), origin_ads_count_in_custom_set));
		if (companion_mode == Ads_Advertisement::ALL_LINKED && ads_in_custom_set.size() < origin_ads_count_in_custom_set)
		{
			ADS_DEBUG((LP_DEBUG, "custom set(%s) is invalid due to the ALL LINKED mode, valida count(%d), origin_ads_count_in_custom_set(%d)\n", ADS_ENTITY_ID_CSTR(custom_set_id), ads_in_custom_set.size(), origin_ads_count_in_custom_set));
			continue;
		}
		if (contains_scheduled_ad || only_contains_display_ad)
		{
			ADS_DEBUG((LP_DEBUG, "custom set(%s) pass the validation,contains_scheduled_ad(%d), only_contains_display_ad(%d)\n",
				ADS_ENTITY_ID_CSTR(custom_set_id), contains_scheduled_ad, only_contains_display_ad));
			std::copy(ads_in_custom_set.begin(), ads_in_custom_set.end(), std::back_inserter(all_companion_candidate_for_validation));
		}
		else
		{
			ADS_DEBUG((LP_DEBUG, "custom set(%s) is invalid, contains_scheduled_ad(%d), only_contains_display_ad(%d)\n",
				ADS_ENTITY_ID_CSTR(custom_set_id), contains_scheduled_ad, only_contains_display_ad));
		}
	}
	return 0;
}

/*************************
 *	description: check_companion_of_scheduled_ad
 *		check scheduled ad whether has valid companion display and check it frequency cap
 *	params:
 *		scheduled_candidate,
 *		vslot, slot which scheduled_candidate will be filled in
 *		check_initial,
 *		fill_as_companion(out)
 *		companion_plan(out) used by the take_companion
 *	return : 0 is ok,<0 is error
 * ****/

int
Ads_Selector::check_companion_of_scheduled_ad(Ads_Selection_Context* ctx,
	const std::map<Ads_GUID, Linking_Set_List_Vector>& placement_custom_sets,
	const Ads_Advertisement_Candidate& scheduled_candidate, Ads_Video_Slot& vslot, bool check_initial,
	bool& fill_as_companion, std::vector<Ads_Slot_Base*>& companion_plan)
{
	if (ctx == nullptr || ctx->request_ == nullptr || scheduled_candidate.ad_ == nullptr)
	{
		ADS_LOG((LP_ERROR, "ctx[%p] or request or scheduled ad[%p] is nullptr\n", ctx, scheduled_candidate.ad_));
		return -1;
	}
	const Ads_Advertisement* scheduled_ad = scheduled_candidate.ad_;
	fill_as_companion = false;
	Ads_GUID placement_id = scheduled_ad->placement_id();
	const Ads_Advertisement* placement = nullptr;
	if (ctx->find_advertisement(placement_id, placement) < 0 || !placement)
	{
		ADS_LOG((LP_ERROR, "placement %s not found\n", ADS_ENTITY_ID_CSTR(placement_id)));
		return -1;
	}

	Ads_Advertisement_Candidate_Vector all_companion_candidate_for_validation;
	if (placement->companion_mode_ != Ads_Advertisement::NOT_LINKED)//normal companion and advance companion
	{
		ADS_DEBUG((LP_DEBUG, "load basic companion or advanced companion ads for ad %s, placement %s\n",
			ADS_ENTITY_ID_CSTR(scheduled_ad->id_), ADS_ENTITY_ID_CSTR(placement->id_)));

		std::copy(scheduled_candidate.companion_candidates_.begin(), scheduled_candidate.companion_candidates_.end(),
					std::back_inserter(all_companion_candidate_for_validation));
	}
	else if (placement->advanced_companion_)//very advance link
	{
		ADS_DEBUG((LP_DEBUG, "load very advanced companion ads for ad %s, placement %s:\n",
			ADS_ENTITY_ID_CSTR(scheduled_ad->id_), ADS_ENTITY_ID_CSTR(placement->id_)));
		do
		{
			auto link_it = placement_custom_sets.find(placement_id);
			if (link_it == placement_custom_sets.end())
			{
				ALOG(LP_DEBUG, "very advance link data is wrong, can not find the custom set, placement(%d)\n",
					ADS_ENTITY_ID_CSTR(placement_id));
				break;
			}
			//1 get custom_set which contains scheduled ad
			//2 get custom_set which only contains the display ad
			//3 merge the 2 set into  combine_set one and call all_companion_candidate_for_validation
			std::map<Ads_GUID, Custom_Linking_Set> link_info_map;
			if (get_custom_set_link_info(ctx, *placement, *(scheduled_ad), link_info_map) < 0)
			{
				ADS_LOG((LP_ERROR, "get_custom_set_link_info error\n"));
				break;
			}
			const Linking_Set_List_Vector& link_set_list = link_it->second;
			if (get_very_advanced_companion_candidate_for_validation(link_set_list, link_info_map,
				all_companion_candidate_for_validation) < 0)
			{
				ADS_LOG((LP_ERROR, "get_very_advanced_companion_candidate_for_validation error\n"));
				break;
			}
		} while(0);
	}

	bool has_valid_companion = false;
	std::multimap<Ads_Slot_Base*, Ads_Advertisement_Candidate*> display_candidate_slot_occupied;//for frequency cap
	if (!all_companion_candidate_for_validation.empty())
	{
		std::map<Ads_Advertisement_Candidate*, Ads_Slot_Base*> avaliable_display_candidate_slot;
		has_valid_companion = (load_scheduled_ad_display_companion_plan(ctx,
								check_initial,
								ctx->player_slots_,
								ctx->page_slots_,
								all_companion_candidate_for_validation,
								avaliable_display_candidate_slot) == 0);
		if (has_valid_companion)
		{
			if (convert_validate_companion_slot_for_plan(scheduled_candidate, vslot, avaliable_display_candidate_slot,
				 companion_plan, display_candidate_slot_occupied) < 0)
			{
				ADS_LOG((LP_ERROR, "convert_validate_companion_slot_for_plan error\n"));
				has_valid_companion = false;
				companion_plan.clear();
				display_candidate_slot_occupied.clear();
			}
		}
	}
	bool frequency_cap_passed = false;
	if (has_valid_companion)
	{
		do
		{
			bool is_companion = true;
			if (!ctx->fc_->pass_frequency_cap_by_slot(ctx, &scheduled_candidate, is_companion, &display_candidate_slot_occupied))
			{
				ADS_DEBUG((LP_DEBUG, "scheduled ad %s frequency cap check failed\n", ADS_ENTITY_ID_CSTR(scheduled_ad->id_)));
				frequency_cap_passed = false;
				break;
			}
			if (!ctx->fc_->pass_frequency_cap_time_based(ctx, &scheduled_candidate, &vslot))
			{
				ADS_DEBUG((LP_DEBUG, "scheduled ad %s time-based frequency cap check failed\n", ADS_ENTITY_ID_CSTR(scheduled_ad->id_)));
				frequency_cap_passed = false;
				break;
			}
			frequency_cap_passed = true;
		} while(0);
	}
	fill_as_companion = has_valid_companion && frequency_cap_passed;
	return 0;
}


int Ads_Selector::initialize_advertisement_reference(Ads_Selection_Context *ctx, const Ads_Creative_Rendition* creative, const Ads_String &oar_ad_watermark
		, bool is_companion, Ads_Advertisement_Candidate_Ref *ref, size_t ref_flags, int position_in_sub_slot)
{
	if (ctx == nullptr || ref == nullptr || ref->candidate_ == nullptr || ref->candidate_->ad_ == nullptr)
	{
		ADS_LOG((LP_ERROR, "ctx is null or ref or ref->candidate_ is null[%p/%p]\n", ctx, ref));
		return -1;
	}
	const Ads_Advertisement_Candidate * candidate = ref->candidate_;
	if (ref->is_external_bridge_ad())
	{
		// Follow programmic logic, as external ad dosen't have real ad ID, only placeholder ID, we need:
		// high 16 bits to identify different ad, low 16 bits to represent its repeat number
		ref->replica_id_ = (candidate->bridge_ad_replica_id() << 16) | candidate->references_.size(); 
	} else
	{
		ref->replica_id_ = candidate->references_.size();
	}
	ref->flags_ |= ref_flags;
	ref->creative_ = creative;
	ref->is_companion_ = is_companion;
	if ((ref->flags_ & Ads_Advertisement_Candidate_Ref::FLAG_LAST_IN_SLOT) > 0)
	{
		ref->position_in_sub_slot_ = Ads_Advertisement_Candidate_Ref::LAST_POSITION;
	}
	else
	{
		ref->position_in_sub_slot_ = position_in_sub_slot;
	}

	if ((ref->flags_ & Ads_Advertisement_Candidate_Ref::FLAG_OAR) > 0) // oar full avail
		ref->oar_ad_watermark_ = oar_ad_watermark;

	calculate_effective_inventory(ctx, ref);
	return 0;
}

int Ads_Selector::fill_normal_scheduled_ad_display_companion(Ads_Selection_Context *ctx, Ads_Schedule_Ad_Info &schedule_instance,
		std::vector<Ads_Slot_Base*> &companion_plan,  bool check_initial, Ads_Advertisement_Candidate_Set &selected_candidates)
{
	Ads_Advertisement_Candidate *scheduled_candidate = schedule_instance.candidate_;
	Ads_Advertisement_Candidate_Ref *scheduled_ad_ref = schedule_instance.ref_;

	if (ctx == nullptr || scheduled_candidate == nullptr || scheduled_ad_ref == nullptr)
	{
		ADS_LOG((LP_ERROR, "ctx[%p] or candidate[%p] or scheduled_ad_ref is null[%p]\n",
				ctx, scheduled_candidate, scheduled_ad_ref));
		return -1;
	}

	size_t flags = 0;
	if (check_initial)
	{
		flags |= FLAG_CHECK_INITIAL;
	}
	int64_t profit = 0;
	bool is_taking_scheduled_ad_companion = true;
	this->take_companion(ctx, scheduled_candidate, scheduled_candidate->companion_candidates_, selected_candidates, companion_plan, flags, profit, is_taking_scheduled_ad_companion);

	std::vector<Ads_Advertisement_Candidate_Ref *> companion_refs;
	companion_refs.push_back(scheduled_ad_ref);

	for (size_t i = 0; i < scheduled_candidate->companion_candidates_.size(); ++i)
	{
		Ads_Advertisement_Candidate *current = scheduled_candidate->companion_candidates_[i];
		if (current == nullptr || current->ad_ == nullptr)
		{
			ADS_LOG((LP_ERROR, "companion candidate[%p] or its ad is nullptr\n", current));
			continue;
		}
		if (current == scheduled_candidate)
		{
			continue; // scheduled_candidate's ref is already created
		}
		Ads_Slot_Base *aslot = companion_plan[i];
		if (aslot == nullptr)
		{
			continue;
		}

		ADS_DEBUG((LP_DEBUG, "ad(%s) will be filled as companion into slot %s\n", ADS_ENTITY_ID_CSTR(current->ad_->id_), aslot->custom_id_.c_str() ));
		const Ads_Creative_Rendition* creative = nullptr;
		Ads_Advertisement_Candidate_Ref *ref = nullptr;
		ref = new Ads_Advertisement_Candidate_Ref(current, aslot);
		int position_in_sub_slot = Ads_Advertisement_Candidate_Ref::INVALID_POSITION;

		size_t ref_flags = Ads_Advertisement_Candidate_Ref::FLAG_SCHEDULE_COMPANION;
		if (initialize_advertisement_reference(ctx, creative, "", true/*is_companion*/, ref, ref_flags, position_in_sub_slot) < 0)
		{
			ADS_LOG((LP_ERROR, "fail to initialize ref, ad:%s, slot:%s\n", ADS_ENTITY_ID_CSTR(current->ad_->id_), aslot->custom_id_.c_str()));
			delete ref;
			ref = nullptr;
			return -1;
		}
		current->used_slots_.insert(ref->slot_);
		current->references_.push_back(ref);
		take_advertisement_candidate_reference(ctx, ref, ctx->delivered_candidates_);
		companion_refs.push_back(ref);
	}

	for (auto *ref : companion_refs)
	{
		ref->companion_candidates_ = companion_refs;
	}
	return 0;
}

// schedule_instance.ref_ should be nullptr when invoking this function and it will remain to be nullptr if this function fails
// schedule_instance.ref_ will be newed by this function, it should be put in somewhere is responsible to delete it
int Ads_Selector::fill_scheduled_video_advertisement(Ads_Selection_Context *ctx, Ads_Schedule_Ad_Info &schedule_instance,
		bool is_fixed_position, int &position_in_sub_slot)
{
	Ads_Video_Slot *vslot = schedule_instance.slot_;
	Ads_Advertisement_Candidate *scheduled_candidate = schedule_instance.candidate_;
	bool is_phantom = schedule_instance.is_phantom_;

	if (ctx == nullptr || vslot == nullptr || scheduled_candidate == nullptr || scheduled_candidate->ad_ == nullptr || schedule_instance.ref_ != nullptr)
	{
		ADS_LOG((LP_ERROR, "ctx[%p] or vslot[%p] or candidate[%p] or candidate ad is null, or ref[%p] isn't null\n", ctx, vslot, scheduled_candidate, schedule_instance.ref_));
		return -1;
	}

	ADS_DEBUG((LP_DEBUG, "Begin Filling scheduled video ad %s, slot %s, is_phantom %d\n",
			ADS_ENTITY_ID_CSTR(schedule_instance.ad_id_), vslot->custom_id_.c_str(), is_phantom));

	if (!is_phantom)
	{
		if (vslot->take_scheduled_advertisement(ctx, scheduled_candidate, schedule_instance.creative_rendition_, is_fixed_position, position_in_sub_slot) < 0)
		{
			ADS_LOG((LP_ERROR, "fail to take scheduled advertisement, slot: %s\n", vslot->custom_id_.c_str()));
			return -1;
		}

		if (schedule_instance.has_attribute(Ads_Schedule_Ad_Info::LAST_IN_BREAK) && !vslot->has_last_in_slot_ad_filled_)
		{
			// AF logic doesn't need to mark slot as LIB ad was filled when request is proposal
			if ((scheduled_candidate->flags() & Ads_Advertisement_Candidate::FLAG_PROPOSAL) == 0)
			{
				vslot->has_last_in_slot_ad_filled_ = true;
			}
		}
	}

	scheduled_candidate->used_slots_.insert(vslot);
	if (ads::entity::is_valid_id(schedule_instance.creative_audience_term_id_))
		scheduled_candidate->slot_creative_audience_terms_[vslot] = schedule_instance.creative_audience_term_id_;
	schedule_instance.ref_ = new Ads_Advertisement_Candidate_Ref(scheduled_candidate, vslot);
	size_t scheduled_ad_ref_flags = Ads_Advertisement_Candidate_Ref::FLAG_STATIC_SCHEDULE;
	ctx->scheduler_->initialize_ad_ref_by_scheduled_instance(schedule_instance, *schedule_instance.ref_, scheduled_ad_ref_flags);
	Ads_String oar_ad_watermark;
	if ((scheduled_ad_ref_flags & Ads_Advertisement_Candidate_Ref::FLAG_OAR) > 0)
	{
		oar_ad_watermark = ctx->scheduler_->get_oar_ad_watermark(Ads_String(scheduled_candidate->ad_->internal_id_.c_str()), ctx->scheduler_->custom_break_id_);
	}
	if (initialize_advertisement_reference(ctx, schedule_instance.creative_rendition_, oar_ad_watermark, false/*is companion*/, schedule_instance.ref_, scheduled_ad_ref_flags, position_in_sub_slot) < 0)
	{
		ADS_LOG((LP_ERROR, "fail to initialize ref, slot:%s\n", vslot->custom_id_.c_str()));
		delete schedule_instance.ref_;
		schedule_instance.ref_ = nullptr;
		return -1;
	}

	scheduled_candidate->references_.push_back(schedule_instance.ref_);
	// scheduled ad has been counted in forward looking, so no ctx->inc_occurences() here
	ADS_DEBUG((LP_DEBUG, "End fill scheduled ad(%s), duration remain:%lu\n", ADS_ENTITY_ID_CSTR(schedule_instance.ad_id_), vslot->duration_remain_));
	return 0;
}

int Ads_Selector::fill_scheduled_advertisement(Ads_Selection_Context &ctx, Ads_Schedule_Ad_Info &schedule_instance, Ads_Advertisement_Candidate_Set &selected_candidates)
{
	Ads_Advertisement_Candidate *candidate = schedule_instance.candidate_;
	Ads_Video_Slot *vslot = schedule_instance.slot_;
	Ads_GUID scheduled_ad_id = schedule_instance.ad_id_;
	if (candidate == nullptr || vslot == nullptr)
	{
		ADS_LOG((LP_ERROR, "candidate[%p] or vslot[%p] is nullptr\n", candidate, vslot));
		return -1;
	}
	if (ctx.request_ == nullptr || ctx.request_->rep() == nullptr)
	{
		ADS_LOG((LP_ERROR, "ctx.request_[%p] or its rep() is nullptr\n", ctx.request_));
		return -1;
	}

	// "candidate->slot_" needs to be non-null to prevent filling the scheduled candidate into the same parent slot
	// by doing so, when filling dynamic ad, find_slot_for_advertisement() will think this candidate is not active.
	candidate->slot_ = vslot;
#if defined(ADS_ENABLE_FORECAST)
	add_proposal_for_hylda_incompatible_candidate(ctx.request_->rep()->flags_, candidate);
#endif

	int ref_position_in_sub_slot = Ads_Advertisement_Candidate_Ref::INVALID_POSITION;
	if (fill_scheduled_video_advertisement(&ctx, schedule_instance, false/*is_fixed_position*/, ref_position_in_sub_slot) < 0)
	{
		ADS_LOG((LP_ERROR, "fail to fill scheduled video ad: %s into slot: %s\n", ADS_ENTITY_ID_CSTR(scheduled_ad_id), vslot->custom_id_.c_str()));
		return -1;
	}
	selected_candidates.insert(candidate);

	if (!schedule_instance.is_phantom_)
	{
		take_advertisement_candidate_reference(&ctx, schedule_instance.ref_, ctx.delivered_candidates_);
	}
	else
	{
		ctx.phantom_candidates_.push_back(schedule_instance.ref_);
	}

	if ((schedule_instance.attribute_ & Ads_Schedule_Ad_Info::EXTERNAL_BRIDGE_AD) > 0)
		return 0;

	// check and fill schedule ad's companion display
	//TODO:need to move check_initial to the Ads_Video_Slot
	bool check_initial = (
		ctx.request_->rep()->site_section_.video_player_.video_.auto_play_ &&
		!ctx.keep_initial_ad() &&
		!vslot->force_first_call() &&
		int64_t(vslot->time_position_) <= ctx.max_initial_time_position_
	);
	bool need_fill_companion_display = false;
	std::vector<Ads_Slot_Base*> companion_plan(candidate->companion_candidates_.size());
	if (check_companion_of_scheduled_ad(&ctx, ctx.placement_custom_sets_, *candidate, *vslot,
			check_initial, need_fill_companion_display, companion_plan) < 0)
	{
		ADS_LOG((LP_ERROR, "companion checking failed will filling ad:%s to slot:%s\n",
				ADS_ENTITY_ID_CSTR(scheduled_ad_id), vslot->custom_id_.c_str()));
		return -1;
	}

	if (need_fill_companion_display)
	{
		ADS_DEBUG((LP_DEBUG, "filling companion display\n"));
		if (schedule_instance.is_phantom_)
		{
			if (fill_phantom_ad_display_companion(&ctx, *candidate, companion_plan, selected_candidates) < 0)
			{
				ADS_LOG((LP_ERROR, "fail to fill phantom ad display companion!\n"));
				return -1;
			}
		}
		else
		{
			if (fill_normal_scheduled_ad_display_companion(&ctx, schedule_instance, companion_plan, check_initial, selected_candidates) < 0)
			{
				ADS_LOG((LP_ERROR, "fail to fill scheduled companion ad\n"));
				return -1;
			}
		}
	}

	return 0;
}

int Ads_Selector::fill_phantom_ad_display_companion(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate &phantom_candidate, std::vector<Ads_Slot_Base*> &companion_plan, Ads_Advertisement_Candidate_Set &selected_candidates)
{
	auto &companion_candidates = phantom_candidate.companion_candidates_;
	if (phantom_candidate.ad_ == nullptr || companion_candidates.size() != companion_plan.size())
	{
		ADS_DEBUG((LP_DEBUG, "phantom_candidate.ad_[%p] is nullptr or companion_candidates size(%d) and companion_plan size(%d) unequal!\n", phantom_candidate.ad_, companion_candidates.size(), companion_plan.size()));
		return -1;
	}

	size_t num_companions = companion_candidates.size();
	for (size_t i = 0; i < num_companions; ++i)
	{
		Ads_Advertisement_Candidate *current = companion_candidates[i];
		if (current == nullptr || current->ad_ == nullptr)
		{
			ADS_DEBUG((LP_DEBUG, "companion candidate[%p] or its ad is nullptr\n", current));
			continue;
		}

		Ads_Slot_Base *slot = companion_plan[i];
		if (slot == nullptr || slot->env() == ads::ENV_VIDEO)
		{
			continue; // skip phantom video ad as its Frequency Cap is counted in forward looking
		}

		ctx->fc_->inc_occurences(current, slot, ctx);
		selected_candidates.insert(current);
	}
	return 0;
}

int
Ads_Selector::try_replicate_selected_candidate_advertisements(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate_List& candidates, Ads_Slot_List & slots, std::list<Ads_Advertisement_Candidate_Ref *>& candidate_refs, int64_t& profit,
		Take_Advertisement_Slot_Position* slot_position, Take_Advertisement_Candidate_Rank* candidate_rank)
{
	Ads_Advertisement_Candidate_Ref_List new_refs;

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG((LP_DEBUG, "Repeat Ad: try repeat ads in slots:%s BEGIN\n", ads::__ids(slots,
			[](Ads_Slot_Base* slot){
				return slot->custom_id_;
		}).c_str()));
		ADS_DEBUG((LP_DEBUG, "original ads: %s\n", ads::__ids(candidates,
			[](Ads_Advertisement_Candidate* candidate){
				return ADS_ENTITY_ID_STR(candidate->ad_->id_);
		}).c_str()));
	}

	bool simple_sort_for_repeat_ad = ctx->repository_->system_config().is_profile_enable_simple_sort_for_repeat_ad(-1);
	if (!simple_sort_for_repeat_ad)
		simple_sort_for_repeat_ad = ctx->profile_ && ctx->repository_->system_config().is_profile_enable_simple_sort_for_repeat_ad(ads::entity::id(ctx->profile_->id_));

#if defined(ADS_ENABLE_FORECAST)
	if (!(ctx->request_flags() & Ads_Request::Smart_Rep::FORCE_PROPOSAL)) // no sort for UGA/GA
#endif
	{
		if (!slots.empty() && simple_sort_for_repeat_ad)
		{
			auto assoc = slots.front()->associate_type();
			std::stable_sort(candidates.begin(), candidates.end(), Ads_Advertisement_Candidate::greater(ctx, assoc, false, true));
		}
	}

	for (Ads_Slot_List::iterator it = slots.begin(); it != slots.end(); ++it)
	{
		Ads_Slot_Base *slot = *it;

		if (!slot->has_profile())
		{
			for (Ads_Advertisement_Candidate_List::const_iterator sit = candidates.begin(); sit != candidates.end(); ++sit)
			{
				Ads_Advertisement_Candidate *candidate = *sit;
				if (candidate->lite_->is_watched()) ctx->log_watched_info(*candidate->lite_, slot, "NO_PROFILE_FOR_SLOT");
			}
			continue;
		}
		if (slot->env() != ads::ENV_VIDEO && !slot->accept_initial()) continue;

		if (!ctx->is_slot_availiable_for_replicate_ad(*slot)) continue;

		Ads_Advertisement_Candidate_List acandidates(candidates);
#if defined(ADS_ENABLE_FORECAST)
		if (!(ctx->request_flags() & Ads_Request::Smart_Rep::FORCE_PROPOSAL)) // no sort for UGA/GA
#endif
		{
			if (simple_sort_for_repeat_ad)
			{
				std::stable_sort(acandidates.begin(), acandidates.end(),
					[](const Ads_Advertisement_Candidate* x, const Ads_Advertisement_Candidate* y)
					{
						if (y->ad_->is_follower()) return false;
						else if (x->ad_->is_follower()) return true;

						return (x->references_.size() < y->references_.size());
					});
			}
			else
			{
				std::stable_sort(candidates.begin(), candidates.end(), Ads_Advertisement_Candidate::greater(ctx, slot->associate_type(), false, true, true));
				acandidates = candidates;
			}
		}

		if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
		{
			ADS_DEBUG((LP_DEBUG, "will try repeat ads: %s in slot:%s\n", ads::__ids(acandidates,
				[](Ads_Advertisement_Candidate* candidate){
					return ADS_ENTITY_ID_STR(candidate->ad_->id_);
			}).c_str(), slot->custom_id_.c_str()));
		}

		Ads_Advertisement_Candidate_List tmp;
		size_t n_repeat = 0;
		while(n_repeat++ < 10)//external ads max repeat times within slot
		{
			Retry_Scheduler candidate_combine_with_retry(acandidates, ctx, slot->associate_type(), false, true, true);

		while (true)
		{
			Ads_Advertisement_Candidate *candidate = candidate_combine_with_retry.get_next_candidate();
			if (candidate == NULL)
				break;
#if defined(ADS_ENABLE_FORECAST)
            if (!(ctx->request_flags() & Ads_Request::Smart_Rep::BYPASS_BUDGET_CHECK) && candidate->ad_->repeat_mode_ == Ads_Advertisement::DO_NOT_REPEAT)
#else
			if (candidate->ad_->repeat_mode_ == Ads_Advertisement::DO_NOT_REPEAT)
#endif
				continue;

			bool repeat_within_slot = false;
#if defined (ADS_ENABLE_FORECAST)
			if (repeat_within_slot && (ctx->request_flags() & Ads_Request::Smart_Rep::BYPASS_RESTRICTION)
					&& !candidate->is_pre_selection_external_translated() /*market ad translated in phase 8 SSTF DO NOT repeat*/)
				candidate->flags_ |= Ads_Advertisement_Candidate::FLAG_REPEAT_WITHIN_SLOT;
			// no inventory reserved, no repeat
			if (ctx->request_flags() & Ads_Request::Smart_Rep::FORCE_PROPOSAL || candidate->is_proposal())
				repeat_within_slot = false;
#endif
			bool recoverable = false;
			int replicate_result = try_replicate_candidate_advertisement(ctx, candidate, slot, new_refs, profit, repeat_within_slot, recoverable);
			if (replicate_result < 0)
				break;
			if (recoverable)
				candidate_combine_with_retry.pend_for_retry(candidate);
			if (replicate_result > 0)
				candidate_combine_with_retry.reactivate();


			if (repeat_within_slot) tmp.push_back(candidate);
		}

		if (tmp.empty()) break;

		acandidates.clear();
		acandidates.swap(tmp);
		}
	}

	for (Ads_Advertisement_Candidate_Ref_List::iterator it = new_refs.begin(); it != new_refs.end(); ++it)
	{
		Ads_Advertisement_Candidate_Ref *ref = *it;
#if defined(ADS_ENABLE_FORECAST)
		this->take_advertisement_candidate_reference(ctx, ref, candidate_refs, slot_position, candidate_rank);
#else
		this->take_advertisement_candidate_reference(ctx, ref, candidate_refs);
#endif
	}

	if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_)
	{
		ADS_DEBUG((LP_DEBUG, "Repeat Ad: try repeat ads in slots:%s END\n", ads::__ids(slots,
             [](Ads_Slot_Base* slot){
                 return slot->custom_id_;
		}).c_str()));
	}

	return 0;
}

int
Ads_Selector::append_fallback_candidate_advertisements(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate_List& candidates,
							std::list<Ads_Advertisement_Candidate_Ref*>& candidate_refs)
{
	bool verbose = (ctx->verbose_ > 0);

	Ads_Advertisement_Candidate_Ref_List refs;

	size_t max_server_side_fallback_size = ctx->repository_->system_config().max_sstf_fallback_chain_length_;
	size_t max_client_side_fallback_size = 0;
	if (ctx->request_->rep()->capabilities_.supports_fallback_ads_)
	{
		max_client_side_fallback_size = 5;
		const char *s = ctx->repository_->system_variable("selector.max_fallback_chain_length");
		if (s) max_client_side_fallback_size = ads::str_to_i64(s);
	}

	/// reset valid flag
	for (Ads_Advertisement_Candidate_List::const_iterator cit = candidates.begin(); cit != candidates.end(); ++cit)
	{
		Ads_Advertisement_Candidate *candidate = *cit;
		candidate->valid2_ = true;
		candidate->slot_ = 0;
	}

	/// sort fallback candidates by VIDEO price
	std::stable_sort(candidates.begin(), candidates.end(), Ads_Advertisement_Candidate::greater(ctx, ads::MEDIA_VIDEO));
	bool all_linked_lwp_no_fallback = false;
	bool match_fallback_ad_duration = false;
	const Ads_Network *cro_network = nullptr;
	const Ads_Repository *repo = ctx->repository_;
	if (ctx->root_asset_ && repo->find_network(ctx->root_asset_->network_id_, cro_network) >= 0 && cro_network && cro_network->config()->all_linked_lwp_no_fallback_)
		all_linked_lwp_no_fallback = true;
	if (ctx->is_live() && cro_network != nullptr && cro_network->config()->match_fallback_ad_duration_)
		match_fallback_ad_duration = true;
	Ads_Slot_Base *pre_slot = nullptr;
	std::unique_ptr<json11::Value> failed_fallback_ad_info;

	bool has_p10_sstf_capability = ctx->enable_server_translation_ || ctx->request_->rep()->capabilities_.enable_vast_translation_;
	bool enable_vast_tag_as_server_side_fallback = !has_p10_sstf_capability &&
		(ctx->root_asset_ && ctx->repository_->network_function("VAST_TAG_AS_SERVER_SIDE_FALLBACK", ctx->root_asset_->network_id_));

	for (std::list<Ads_Advertisement_Candidate_Ref *>::iterator it = candidate_refs.begin(); it != candidate_refs.end(); ++it)
	{
		Ads_Advertisement_Candidate_Ref *aref = *it;
		Ads_Slot_Base *slot = aref->slot_;
		ADS_ASSERT(aref && slot);

		if (!aref->fallback_candidates_.empty()) continue;

		/// only process driving ad_ref
		if (aref->is_companion_) continue;

		const Ads_Advertisement_Candidate *acandidate = aref->candidate_;
		ADS_ASSERT(acandidate);

		auto fallback_side = aref->fallback_side(*ctx);

		size_t max_fallback_size = (fallback_side == Ads_Selection_Context::SERVER_SIDE_AD_FALLBACK ? max_server_side_fallback_size
						: (fallback_side == Ads_Selection_Context::CLIENT_SIDE_AD_FALLBACK ?
							(ctx->force_fallback_ad_num_ > 0 ? ctx->force_fallback_ad_num_ : max_client_side_fallback_size) : 0));
		if (max_fallback_size <= 0)
			continue;

		if (fallback_side == Ads_Selection_Context::SERVER_SIDE_AD_FALLBACK)
			aref->flags_ |= Ads_Advertisement_Candidate_Ref::FLAG_SSTF_FALLBACK_APPLIED;

		if (acandidate->is_proposal()) continue; // proposal doesn't have fallback ads

		// configured ad doesn't have fallback ads
		if (aref->is_scheduled_delivered() || aref->is_replacement()) continue;

		// special ads in AF doesn't have fallback ads
		if (acandidate->ad_->is_portfolio() || acandidate->ad_->is_custom_portfolio())
			continue;
		// backup vslot's mask
		Ads_Video_Slot *vslot = 0;
		uint64_t masks = 0;
		bool check_initial = false;

		if (slot->env() == ads::ENV_VIDEO)
		{
			vslot = reinterpret_cast<Ads_Video_Slot *>(slot);
			masks = vslot->masks_;

			auto it = acandidate->slot_masks_.find(slot);
			if (it != acandidate->slot_masks_.end())
				vslot->masks_ &= ~(it->second);

			if (ctx->request_->rep()->site_section_.video_player_.video_.auto_play_
				&& !ctx->keep_initial_ad()
				&& !slot->force_first_call()
				&& int64_t(vslot->time_position_) <= ctx->max_initial_time_position_)
				check_initial = true;
		}

		Ads_Advertisement_Candidate_Set selected_candidates;
		size_t count = 0;

		aref->flags_ |= Ads_Advertisement_Candidate_Ref::FLAG_DRIVING_FALLBACK_AF;
		if (fallback_side == Ads_Selection_Context::CLIENT_SIDE_AD_FALLBACK)
			aref->flags_ |= Ads_Advertisement_Candidate_Ref::FLAG_CLIENT_FALLBACK_ENABLED_AF;

		if (match_fallback_ad_duration && pre_slot != slot)
		{
			std::stable_sort(candidates.begin(), candidates.end(), Ads_Advertisement_Candidate::duration_greater(ctx, slot));
			pre_slot = slot;
		}

		ADS_DEBUG((LP_DEBUG, "append fallback: primary ad %s can have max %d fallback ads\n", CANDIDATE_LITE_LPID_CSTR(acandidate->lite_), max_fallback_size));
#define INIT_FALLBACK_AD_DEBUG_INFO_COLLECTION(PRIMARY_AD) \
		if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_) \
		{ \
			failed_fallback_ad_info.reset(new json11::Value); \
			(*failed_fallback_ad_info)["ad_prem"] = ads::entity::id((PRIMARY_AD)->ad_id()); \
		}

#define COLLECT_FAILED_FALLBACK_AD(CANDIDATE, MESSAGE) \
		if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_) \
		{ \
			if (failed_fallback_ad_info != nullptr)  \
			{ \
				json11::Object ad = { \
					{"ad", ads::entity::id((CANDIDATE)->ad_id())}, \
					{"msg", MESSAGE}, \
				}; \
				(*failed_fallback_ad_info)["fail_fallbacks"].append(ad); \
			} \
		}

#define OUTPUT_FAILED_FALLBACK_AD() \
		if (Ads_Server_Config::instance()->enable_debug_ && ctx->verbose_) \
		{ \
			if (failed_fallback_ad_info != nullptr) \
			{ \
				ADS_DEBUG((LP_DEBUG, "append fallback: %s\n", failed_fallback_ad_info->to_string().c_str())); \
			} \
		}
		INIT_FALLBACK_AD_DEBUG_INFO_COLLECTION(aref);
		for (Ads_Advertisement_Candidate_List::const_iterator cit = candidates.begin(); cit != candidates.end(); ++cit)
		{
			if (count >= max_fallback_size) break;

			Ads_Advertisement_Candidate *candidate = *cit;
			ADS_ASSERT(candidate);

			if (candidate == acandidate) continue;

			if (candidate->used_slots_.find(slot) != candidate->used_slots_.end() || candidate->is_parent_slot_used(slot))
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "overlapped slot");
				continue;
			}

			const Ads_Advertisement_Candidate* selected_candidate = ctx->selected_candidates_.candidate_with_same_id(candidate);
			// candidate cannot be fallback ad, since another candidate with same ad id has been selected
			if (selected_candidate != nullptr && selected_candidate != candidate)
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "same ad id with other global selected ad");
				continue;
			}
			selected_candidate = selected_candidates.candidate_with_same_id(candidate);
			if (selected_candidate != nullptr)
			{
				//ADS_ASSERT(selected_candidate != candidate); // failed assertion
				COLLECT_FAILED_FALLBACK_AD(candidate, "same ad id with other selected ad under current primary ad");
				continue;
			}

			// follower won't be fallback ads.
			if (candidate->ad_->is_follower()) continue;

			if (!candidate->active(ads::MEDIA_VIDEO) && !candidate->active(ads::MEDIA_SITE_SECTION))
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "inactive with MEDIA_VIDEO/MEDIA_SITE_SECTION");
				continue;
			}
			if (candidate->is_proposal()) continue;

			if (candidate->ad_->is_portfolio() || candidate->ad_->is_custom_portfolio())
				continue;

			if (candidate->is_pod_ad()
				|| (candidate->is_pre_selection_external_translated() && !candidate->references_.empty() /*&& !candidate->is_multiround_two_phase_translated_ad()*/))
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "due to pod ad or repeated external translate ad");
				continue;
			}

			auto fallback_ad_fallback_side = candidate->fallback_side();
			if (fallback_ad_fallback_side == Ads_Selection_Context::SERVER_SIDE_AD_FALLBACK)
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "due to P10 sstf ad");
				continue;
			}

			if (fallback_side == Ads_Selection_Context::SERVER_SIDE_AD_FALLBACK && fallback_ad_fallback_side != Ads_Selection_Context::NO_AD_FALLBACK)
			{
				if (enable_vast_tag_as_server_side_fallback && !aref->candidate_->is_vast_tag() && candidate->is_vast_tag() && !candidate->useVTF(slot))
				{
					// feature of FW-53755
					// if enable the network function and player supports parsing vast in client side
					// the vast tag without useVTF=1 can be the fallback ad of the driven ad which is not vast tag ad
					// so let the candidate do following checking
				}
				else
				{
					COLLECT_FAILED_FALLBACK_AD(candidate, "due to ssvt ad or wrapper ad can't be server side fallback ad" );
					continue;
				}
			}

			if (all_linked_lwp_no_fallback && candidate->is_companion())
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "due to all_linked_lwp_no_fallback");
				continue;
			}

			// compare position_in_slot with ref's position in slot
			if (vslot)
			{
				Ads_Video_Slot *aslot = (vslot->parent_ ? vslot->parent_  : vslot);

				if (candidate->ad_unit() && candidate->ad_unit()->position_in_slot_ != 0)
				{
					if (candidate->ad_unit()->position_in_slot_ > 0 && candidate->ad_unit()->position_in_slot_ <= aref->position_in_slot_)
					{
						COLLECT_FAILED_FALLBACK_AD(candidate, "positive position_in_slot not match " +
								std::to_string(candidate->ad_unit()->position_in_slot_) + " <= " +
								std::to_string(aref->position_in_slot_));
						continue;
					}
					if (candidate->ad_unit()->position_in_slot_ < 0 && candidate->ad_unit()->position_in_slot_ > aref->position_in_slot_ - (int)aslot->num_advertisements_)
					{
						COLLECT_FAILED_FALLBACK_AD(candidate, "negative position_in_slot not match " +
								std::to_string(candidate->ad_unit()->position_in_slot_) + " > " +
								std::to_string(aref->position_in_slot_) + "-" +  std::to_string(aslot->num_advertisements_));

						continue;
					}
				}

				Creative_Rendition_List temp;
				for (Creative_Rendition_List::const_iterator it = candidate->applicable_creatives(vslot).begin();
					 it != candidate->applicable_creatives(vslot).end(); ++it)
				{
					const Ads_Creative_Rendition *rendition = *it;
					if (rendition->clearcast_info().flag_ & Ads_Creative_Rendition::NOT_FIRST && aref->position_in_slot_ == 0) continue;
					if (rendition->clearcast_info().flag_ & Ads_Creative_Rendition::NOT_LAST && aslot->num_advertisements_ == (size_t)aref->position_in_slot_ + 1) continue;

					temp.push_back(rendition);
				}
				if (temp.empty())
				{
					COLLECT_FAILED_FALLBACK_AD(candidate, "no creatives");
					continue;
				}
				else candidate->applicable_creatives(vslot).swap(temp);
			}

			if (!candidate->slot_ref_private(slot))
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "no applicable slot");
				continue;
			}

			// standalone display won't fallback to companion ad
			if (!acandidate->ad_unit()->is_temporal() && candidate->is_companion())
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "companion ad is not allowed for primary standalone display ad");
				continue;
			}

			if (!candidate->references_.empty() && candidate->ad_->repeat_mode_ == Ads_Advertisement::DO_NOT_REPEAT)
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "disallow repeat");
				continue;
			}

			if (vslot && candidate->duration(vslot) > vslot->duration_remain() + acandidate->duration(vslot))
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "break slot duration");
				continue;
			}

			if (ctx->limit_fallback_ad_duration_ && vslot && candidate->duration(vslot) > acandidate->duration(vslot))
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "duration larger than primary ad");
				continue;
			}

			if (match_fallback_ad_duration && vslot && candidate->duration(vslot) > acandidate->duration(vslot))
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "duration mismatch primary ad");
				continue;
			}


			if (!this->test_advertisement_slot_restrictions(ctx, candidate, slot, 0, verbose))
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "failed slot restriction");
				continue;
			}


			if (!this->test_advertisement_unit_sponsorship(candidate->lite_, slot, verbose))
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "failed ad unit sponsorship");
				continue;
			}

			if (!candidate->test_advertisement_exclusivity(ctx, ctx->selected_candidates_, slot))
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "failed ALL/TARGET AD UNIT exclusivity");
				continue;
			}

			bool satisfied = true;

			std::vector<std::pair<Ads_Advertisement_Candidate*, Ads_Slot_Base *> > plan;
			std::multimap<Ads_Slot_Base *, Ads_Advertisement_Candidate *> occupied;

			if (candidate->is_companion() && candidate->ad_unit()->is_temporal())
			{
				for (Ads_Advertisement_Candidate_Vector::iterator ait = candidate->companion_candidates_.begin();
					 ait != candidate->companion_candidates_.end(); ++ait)
				{
					Ads_Advertisement_Candidate *current = *ait;
					Ads_Slot_Base *slot = 0;

					if (current->ad_unit()->is_temporal()) continue;

					size_t flags = FLAG_CHECK_COMPANION;
					if (check_initial) flags |= FLAG_CHECK_FALLBACK_INITIAL;

					if ((this->find_slot_for_advertisement(ctx, current, ctx->player_slots_, ctx->selected_candidates_, occupied, slot, flags) >= 0 && slot)
						|| (this->find_slot_for_advertisement(ctx, current, ctx->page_slots_, ctx->selected_candidates_, occupied, slot, flags) >= 0 && slot))
					{
						plan.push_back(std::make_pair(current, slot));
						occupied.insert(std::make_pair(slot, current));
						continue;
					}

					satisfied = false;
					break;
				}
			}
 
			if (!satisfied)
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "failed to fill companion display ads");
				continue;
			}

			if (!ctx->fc_->pass_frequency_cap_by_slot(ctx, candidate) || !ctx->fc_->pass_frequency_cap_time_based(ctx, candidate, slot))
			{
				COLLECT_FAILED_FALLBACK_AD(candidate, "failed for frequency cap checking");
				continue;
			}

			//decision info
			if (match_fallback_ad_duration && count == 0 && candidate->duration(slot) < acandidate->duration(slot))
				ctx->decision_info_.set_flag2(Decision_Info::INFO_2__FLAG_8);

			{
				Ads_Advertisement_Candidate_Ref *ref = new Ads_Advertisement_Candidate_Ref(candidate, slot);
				ref->replica_id_ = candidate->references_.size();
				ref->is_fallback_ = true;
				ref->driven_candidate_ = aref;
				ref->position_in_slot_ = aref->position_in_slot_;

				candidate->used_slots_.insert(ref->slot_);
				candidate->references_.push_back(ref);

				this->calculate_effective_inventory(ctx, ref);
				selected_candidates.insert(candidate);
				refs.push_back(ref);

				aref->fallback_candidates_.push_back(ref);

				for (auto it = plan.begin(); it != plan.end(); ++it)
				{
					Ads_Advertisement_Candidate *current = it->first;
					Ads_Slot_Base *slot = it->second;

					Ads_Advertisement_Candidate_Ref *cref = new Ads_Advertisement_Candidate_Ref(current, slot);
					cref->replica_id_ = current->references_.size();
					cref->is_fallback_ = true;
					cref->is_companion_ = true;

					current->used_slots_.insert(cref->slot_);
					current->references_.push_back(cref);

					this->calculate_effective_inventory(ctx, cref);
					selected_candidates.insert(current);
					refs.push_back(cref);

					cref->companion_candidates_.push_back(ref);
					ref->companion_candidates_.push_back(cref);
				}

				ADS_DEBUG0((LP_TRACE,
				            "append fallback: adding %s as a %s side fallback for %s\n",
				            CANDIDATE_LITE_LPID_CSTR(candidate->lite_),
				            fallback_side == Ads_Selection_Context::CLIENT_SIDE_AD_FALLBACK ? "client" : "server",
				            CANDIDATE_LITE_LPID_CSTR(acandidate->lite_)
							));

				ctx->fc_->inc_occurences(candidate, slot, ctx);
				if (fallback_ad_fallback_side == Ads_Selection_Context::NO_AD_FALLBACK && ctx->force_fallback_ad_num_ <= 0)
				{
					ADS_DEBUG((LP_DEBUG, "append fallback: stop finding fallback for %s, since fallback ad %s is guaranteed quality\n",
				            CANDIDATE_LITE_LPID_CSTR(acandidate->lite_),
				            CANDIDATE_LITE_LPID_CSTR(candidate->lite_)));
					break;
				}
				aref->flags_ |= Ads_Advertisement_Candidate_Ref::FLAG_CLIENT_FALLBACK_ENABLED_AF;

				++count;
			}
		}
		OUTPUT_FAILED_FALLBACK_AD();
#undef INIT_FALLBACK_AD_DEBUG_INFO_COLLECTION
#undef COLLECT_FAILED_FALLBACK_AD
#undef OUTPUT_FAILED_FALLBACK_AD

		// restore vslot's mask
		if (vslot) vslot->masks_ = masks;

		// update FLAG for really_no_ad statistics
		if (!aref->fallback_candidates_.empty())
		{
			aref->flags_ |= Ads_Advertisement_Candidate_Ref::FLAG_HAS_FALLBACK;

			for (Ads_Advertisement_Candidate_Ref_Vector::iterator it = aref->companion_candidates_.begin();
				 it != aref->companion_candidates_.end(); ++it)
			{
				(*it)->flags_ |= Ads_Advertisement_Candidate_Ref::FLAG_HAS_FALLBACK;
			}

			Ads_Advertisement_Candidate_Ref_List::reverse_iterator fit = aref->fallback_candidates_.rbegin();
			++fit;

			for (; fit != aref->fallback_candidates_.rend(); ++fit)
			{
				Ads_Advertisement_Candidate_Ref *fallback = *fit;
				fallback->flags_ |= Ads_Advertisement_Candidate_Ref::FLAG_HAS_FALLBACK;

				for (Ads_Advertisement_Candidate_Ref_Vector::iterator it = fallback->companion_candidates_.begin();
					 it != fallback->companion_candidates_.end(); ++it)
				{
					(*it)->flags_ |= Ads_Advertisement_Candidate_Ref::FLAG_HAS_FALLBACK;
				}
			}
		}

		ctx->selected_candidates_.insert(selected_candidates.begin(), selected_candidates.end());
	}

	//candidate_refs.insert(candidate_refs.end(), refs.begin(), refs.end());
	for (Ads_Advertisement_Candidate_Ref_List::iterator it = refs.begin(); it != refs.end(); ++it)
	{
		Ads_Advertisement_Candidate_Ref *ref = *it;
		this->take_advertisement_candidate_reference(ctx, ref, candidate_refs);
	}

	return 0;
}

int
Ads_Selector::notify_invalid_advertisement_creative_status(Ads_GUID id, const Ads_String& msg)
{
	bool alarm = false;

	{
		time_t now = ::time(NULL);
		ads::Guard __g(this->invalid_advertisement_states_mutex_);

		if (now - this->invalid_advertisement_states_[id] > 24 * 60 * 60) //every day
		{
			this->invalid_advertisement_states_[id] = now;
			alarm = true;
		}
	}

	if (alarm)
	{
		Ads_String tag = (ads::entity::type(id) == ADS_ENTITY_TYPE_ADVERTISEMENT ? "AD" : "CR");
		Ads_String err = tag + "=" + ADS_ENTITY_ID_STR(id);
		err += "#E=" + msg;

		ADS_LOG((LP_ERROR, "Invalid advertisement/creative: %s\n", err.c_str()));
		Ads_Error_Stats::instance()->err(Ads_Error_Stats::ERR_CR_INVALID);

		Ads_Monitor::STATIC_METRICS metric_type = (msg == "BEHIND_COUNTER") ? Ads_Monitor::ADVERTISEMENT_COUNTER_BEHIND_RPT
									: (msg == "DISCREPANT_COUNTER") ? Ads_Monitor::ADVERTISEMENT_DISCREPANT_COUNTER
									: Ads_Monitor::CREATIVE_INVALID;
		Ads_Monitor::stats_inc(metric_type);
	}

	return 0;
}

int
Ads_Selector::notify_selection_listeners(Ads_Selection_Context *ctx, Ads_Selection_Listener::STAGE stage, void *arg)
{
	if (!ctx->listeners_) return -1;
	for (std::list<Ads_Selection_Listener *>::const_iterator it = ctx->listeners_->begin();
	        it != ctx->listeners_->end();
	        ++it)
	{
		Ads_Selection_Listener *listener = *it;
		if (listener && listener->interested(stage))
			listener->notify(this, ctx, stage, arg);
	}

	return 0;
}

bool
Ads_Selector::is_effective_sponsorship(const Ads_Slot_List & request_slots, const Ads_GUID ad_id) const
{
	for (auto it = request_slots.begin();
		 it != request_slots.end();
		 ++it)
	{
		const Ads_Slot_Base *slot = *it;
		if (slot->sponsor_candidates_.find(ad_id) != slot->sponsor_candidates_.end())
			return true;
	}

	return false;
}

bool
Ads_Selector::test_advertisement_exclusivity(Ads_Selection_Context *ctx, const Ads_Advertisement* ad, const Ads_GUID_Set& filters, bool cross_request) const
{
	if (ad == nullptr) return false;
	// early escape
	if (ad->is_tracking())
	{
		// always true for Embedded
		if (ad->is_embedded())
			return true;
		// always true if ad in sponsor_ads_ or partial_sponsor_ads_ under Own & Operate situation
		if ((ctx->root_network_id(ads::MEDIA_VIDEO) == ctx->root_network_id(ads::MEDIA_SITE_SECTION)) && is_effective_sponsorship(ctx->request_slots_, ad->id_))
			return true;
	}

	if (ctx->exempt_variable_ad_for_sponsorship() && ad->is_variable())
		return true;

#if defined (ADS_ENABLE_FORECAST)
	bool is_excluded = false;
#endif
	for (Ads_GUID_Set::const_iterator it = filters.begin(); it != filters.end(); ++it)
	{
		const Ads_Advertisement *filter = 0;
		if (ctx->find_advertisement(*it, filter) < 0 || !filter)
			continue;

		Ads_Advertisement::EXCLUDE_REASON er;
		bool is_watched = ctx->is_watched(*ad);
		if (!filter->is_compatible_with(ad, false, er, nullptr))
		{
#if defined (ADS_ENABLE_FORECAST)
			if (!is_excluded)
#endif
			{
				char msg[0xff];
				::snprintf(msg, sizeof msg, "EXCLUDED_BY %s : %d", ADS_ENTITY_ID_CSTR(filter->id_), (int)er);
				if (is_watched) ctx->log_watched_info(*ad, msg);
			}

#if defined (ADS_ENABLE_FORECAST)
			if (ctx->scheduler_->is_digital_live_schedule() && cross_request)
			{
				bool looped = ads::entity::is_looped_id(ad->id_);
				for (Ads_GUID_RSet::const_iterator it = ad->children_.begin(); it != ad->children_.end(); ++it)
				{
					Ads_GUID child_id = ads::entity::make_looped_id(*it, looped);

					const Ads_Advertisement *child = 0;
					if (ctx->find_advertisement(child_id, child) < 0 || !child) continue;

					// skip those who have targeting criteria but not in the candidate list
					if (child->targeting_criteria_ != ad->targeting_criteria_) continue;

					if (ctx->scheduler_->airing_current_break_advertisements_.find(child_id) == ctx->scheduler_->airing_current_break_advertisements_.end())
					{
						ctx->hylda_exclusivity_ads_.insert(std::make_pair(filter->id_, child_id));
						ALOG(LP_DEBUG, "hylda cross request exclusivity, %s excluded by %s\n", ADS_ENTITY_ID_CSTR(child_id), ADS_ENTITY_ID_CSTR(filter->id_));
					}
				}
				is_excluded = true;
			}
			else
#endif
				return false;
		}
		else if	(!ad->is_compatible_with(filter, false, er, nullptr))
		{
#if defined (ADS_ENABLE_FORECAST)
			if (!is_excluded)
#endif
			{
				char msg[0xff];
				::snprintf(msg, sizeof msg, "EXCLUDING %s : %d", ADS_ENTITY_ID_CSTR(filter->id_), (int)er);
				if (is_watched) ctx->log_watched_info(*ad, msg);
			}

#if defined (ADS_ENABLE_FORECAST)
			if (ctx->scheduler_->is_digital_live_schedule() && cross_request)
				is_excluded = true;
			else
#endif
				return false;
		}
	}
#if defined (ADS_ENABLE_FORECAST)
	if (is_excluded)
		return false;
#endif
	return true;
}

bool
Ads_Selector::test_advertisement_history_exclusivity(Ads_Selection_Context *ctx, const Ads_Advertisement* ad)
{
	if (ad->type_ == Ads_Advertisement::AD_UNIT && ctx->request_->is_candidate_excluded(ad->id_))
	{
		ADS_DEBUG((LP_TRACE, "%s failed [EXCLUDED CANDIDATE, all=%s]\n",  ADS_ADVERTISEMENT_ID_CSTR(ad->id_), ctx->request_->excluded_candidates().c_str()));
		return false;
	}
	// disable cross request exclusivity for type C by default
	if (ctx->request_->rep()->capabilities_.synchronize_multiple_requests_)
	{
		if (!ctx->request_->user()->page_advertisements_.empty())
		{
			if (!test_advertisement_exclusivity(ctx, ad, ctx->request_->user()->page_advertisements_))
			{
				ADS_DEBUG((LP_TRACE, "%s failed (PAGE)\n", ad->get_ad_type_and_id().c_str()));
				return false;
			}
		}

		if (!ctx->request_->user()->video_advertisements_.empty())
		{
			if (!test_advertisement_exclusivity(ctx, ad, ctx->request_->user()->video_advertisements_))
			{
				ADS_DEBUG((LP_TRACE, "%s failed (VIDEO)\n", ad->get_ad_type_and_id().c_str()));
				return false;
			}
		}

#if defined (ADS_ENABLE_FORECAST)
		if ((!ctx->scheduler_->airing_cross_break_advertisements_.empty()) && (!ad->is_portfolio()))
		{
			if (!test_advertisement_exclusivity(ctx, ad, ctx->scheduler_->airing_cross_break_advertisements_, true))
			{
				ADS_DEBUG((LP_TRACE, "%s failed (HYLDA)\n", ADS_ADVERTISEMENT_ID_CSTR(ad->id_)));
				return false;
			}
		}
#else
		if (!ctx->scheduler_->future_scheduled_ads_.empty())
		{
			if (!test_advertisement_exclusivity(ctx, ad, ctx->scheduler_->future_scheduled_ads_))
			{
				ADS_DEBUG((LP_TRACE, "%s failed (HYLDA)\n", ad->get_ad_type_and_id().c_str()));
				return false;
			}
		}
#endif
	}

	if (!ctx->request_->rep()->capabilities_.synchronize_site_section_slots_)
		return true;

	if (!ctx->request_->page_random().empty())
	{
		std::map<Ads_String, Ads_User_Info::Request_Page_Context *>::iterator it = ctx->user_->pages_.find(ctx->request_->page_random());
		if (it != ctx->user_->pages_.end())
		{
			Ads_User_Info::Request_Page_Context *page = it->second;
			if (!test_advertisement_exclusivity(ctx, ad, page->advertisements_))
			{
				ADS_DEBUG((LP_TRACE, "%s failed (PAGE)\n", ad->get_ad_type_and_id().c_str()));
				return false;
			}
		}
	}

	if (!ctx->request_->video_random().empty())
	{
		std::map<Ads_String, Ads_User_Info::Request_Page_Context *>::iterator it = ctx->user_->videos_.find(ctx->request_->video_random());
		if (it != ctx->user_->videos_.end())
		{
			Ads_User_Info::Request_Page_Context * video = it->second;
			if (!test_advertisement_exclusivity(ctx, ad, video->advertisements_))
			{
				ADS_DEBUG((LP_TRACE, "%s failed (VIDEO)\n",  ad->get_ad_type_and_id().c_str()));
				return false;
			}
		}
	}

	return true;
}

bool
Ads_Selector::test_advertisement_external_access_rule(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate_Lite *ad_lite)
{
	return !ad_lite->is_external() || ad_lite->is_mkpl_programmatic_placeholder()
		|| ad_lite->owner_.test_advertisement_external_access_rule(ctx, ad_lite);
}

bool
Ads_Selector::is_advertisement_in_blacklist(Ads_Selection_Context *ctx, const Ads_Advertisement* ad)
{
	if (ad == nullptr)
		return false;
	// TODO(pfliu): Consider if this function should be moved to Ads_Advertisement_Candidate_Lite
	if (ctx->advertisement_blacklist_
		&& ctx->advertisement_blacklist_->find(ad->id_) != ctx->advertisement_blacklist_->end())
	{
		ADS_DEBUG((LP_DEBUG, "ad %s in blacklist\n", ADS_ENTITY_ID_CSTR(ad->id_)));
		return true;
	}

	if (!ctx->allowed_candidates_.empty()
		&& ad->type_ == Ads_Advertisement::AD_UNIT
		&& ctx->allowed_candidates_.find(ad->id_) == ctx->allowed_candidates_.end())
	{
		ADS_DEBUG((LP_DEBUG, "ad %s not in allowed list\n", ADS_ENTITY_ID_CSTR(ad->id_)));
		return true;
	}

	if (!ctx->disallowed_candidates_.empty()
		&& ctx->disallowed_candidates_.find(ad->id_) != ctx->disallowed_candidates_.end())
	{
		ADS_DEBUG((LP_DEBUG, "testing ad %s in disallowed list\n", ADS_ENTITY_ID_CSTR(ad->id_)));
		return true;
	}

	if (ad->is_testing()
		&& !ctx->allowed_testing_candidates_.empty()
		&& ad->type_ == Ads_Advertisement::AD_UNIT
		&& ctx->allowed_testing_candidates_.find(ad->id_) == ctx->allowed_testing_candidates_.end())
	{
		ADS_DEBUG((LP_DEBUG, "testing ad %s not in allowed list\n", ADS_ENTITY_ID_CSTR(ad->id_)));
		return true;
	}

	return false;
}

int
Ads_Selector::try_merge_bumper(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate_Lite *ad_lite)
{
	if (ad_lite->priority() > ctx->targeted_bumper_.priority_)
	{
		ADS_DEBUG((LP_DEBUG, "bumper ad: find higher priority bumper ad, update context bumper. current context bumper placement id %s priority %d, new context bumper placement id %s priority %d.\n",
			ADS_ENTITY_ID_CSTR(ctx->targeted_bumper_.placement_id_),
			ctx->targeted_bumper_.priority_,
			ADS_ENTITY_ID_CSTR(ad_lite->placement_id()),
			ad_lite->priority()));
		ctx->targeted_bumper_.placement_id_ = ad_lite->placement_id();
		ctx->targeted_bumper_.priority_ = ad_lite->priority();
		ctx->targeted_bumper_.front_ad_ = nullptr;
		ctx->targeted_bumper_.end_ad_ = nullptr;
	}

	if (ad_lite->placement_id() == ctx->targeted_bumper_.placement_id_)
	{
		if (ad_lite->ad_unit()->name_ == "_fw_front_bumper")
		{
			ctx->targeted_bumper_.front_ad_ = ad_lite;
			ADS_DEBUG((LP_DEBUG, "bumper ad: update start bumper ad to %s.\n", ADS_ENTITY_ID_CSTR(ad_lite->id())));
		}

		else if (ad_lite->ad_unit()->name_ == "_fw_end_bumper")
		{
			ctx->targeted_bumper_.end_ad_ = ad_lite;
			ADS_DEBUG((LP_DEBUG, "bumper ad: update end bumper ad to %s.\n", ADS_ENTITY_ID_CSTR(ad_lite->id())));
		}
		else
		{
			ADS_DEBUG((LP_DEBUG, "bumper ad: doesn't update bumper ad to %s due to ad unit name not match, should be _fw_front_bumper or _fw_end_bumper, current ad unit name %s.\n",
				ADS_ENTITY_ID_CSTR(ad_lite->id()),
				ad_lite->ad_unit()->name_.c_str()));
		}
	}
	return 1;
}

int
Ads_Selector::append_bumpers(Ads_Selection_Context *ctx, std::list<Ads_Advertisement_Candidate_Ref *>& candidate_refs)
{
	if (!ads::entity::is_valid_id(ctx->targeted_bumper_.placement_id_)) return 0;

	Ads_Advertisement_Candidate_Lite *fb_lite = ctx->targeted_bumper_.front_ad_;
	Ads_Advertisement_Candidate_Lite *eb_lite = ctx->targeted_bumper_.end_ad_;

	if (fb_lite && eb_lite)
	{
		Ads_Asset_Section_Closure *closure = fb_lite->owner_.to_closure();
		if (!closure)
		{
			ADS_DEBUG((LP_TRACE, "closure for network %s not found\n", ADS_ENTITY_ID_CSTR(ctx->root_section_->network_id_)));
			return -1;
		}

		ADS_ASSERT(closure);

		{
			Ads_Advertisement_Candidate *candidate = new Ads_Advertisement_Candidate(ads::MEDIA_NONE, fb_lite);

			for (Ads_Advertisement::Rendition_RVector::const_iterator it = fb_lite->creatives().begin(); it != fb_lite->creatives().end(); ++it)
			{
				const Ads_Creative_Rendition *creative = it->first;
				if (!ctx->in_date_range(it->second.start_date, it->second.end_date)) continue;
				if (creative->duration_type_ != Ads_Creative_Rendition::VARIABLE && creative->duration_ > (size_t)candidate->duration_)
					candidate->duration_ = creative->duration_ ;
				candidate->creatives_.push_back(creative);
			}
			ctx->bumper_.first = candidate;
			ctx->all_final_candidates_.push_back(candidate);
		}

		{
			Ads_Advertisement_Candidate *candidate = new Ads_Advertisement_Candidate(ads::MEDIA_NONE, eb_lite);

			for (Ads_Advertisement::Rendition_RVector::const_iterator it = eb_lite->creatives().begin(); it != eb_lite->creatives().end(); ++it)
			{
				const Ads_Creative_Rendition *creative = it->first;
				if (!ctx->in_date_range(it->second.start_date, it->second.end_date)) continue;
				if (creative->duration_type_ != Ads_Creative_Rendition::VARIABLE && creative->duration_ > (size_t)candidate->duration_)
					candidate->duration_ = creative->duration_ ;
				candidate->creatives_.push_back(creative);
			}
			ctx->bumper_.second = candidate;
			ctx->all_final_candidates_.push_back(candidate);
		}
	}
	else
	{
		ADS_DEBUG((LP_DEBUG, "bumper ad: skip append bumper, front bumper (%p) & end bumper (%p) must appear in pair.\n",
			fb_lite,
			eb_lite));
		return 0;
	}
	// front & end bumpers MUST be different ad_tree_node
	size_t content_duration = 0;
	Ads_User_Info::Section_Context_Record* scr = ctx->user_->find_section_context_record(ctx->user_->website_root_id());
	if (scr != nullptr)
		content_duration = scr->content_duration_;

	// filter slots full of "null/null" ad
	std::set<const Ads_Video_Slot *> non_empty_slots;
	for (Ads_Advertisement_Candidate_Ref_List::const_iterator it = ctx->delivered_candidates_.begin();
		 it != ctx->delivered_candidates_.end(); ++it)
	{
		const Ads_Advertisement_Candidate_Ref *ref = *it;
		if (ref->creative_ && ads::entity::id(ref->creative_->content_type_id_) != 36 && ref->slot_->env() == ads::ENV_VIDEO)
		{
			const Ads_Video_Slot* slot = reinterpret_cast<const Ads_Video_Slot *>(ref->slot_);
			non_empty_slots.insert(slot->parent_ ? slot->parent_ : slot);
		}
	}

	std::set<Ads_Video_Slot *> processed_slots;
	for (Ads_Slot_List::iterator it = ctx->video_slots_.begin(); it != ctx->video_slots_.end(); ++it)
	{
		Ads_Video_Slot *slot = reinterpret_cast<Ads_Video_Slot *>(*it);
		ADS_ASSERT(slot);

		size_t num_advertisements = slot->num_advertisements_;

		if (slot->parent_)
		{
			if (processed_slots.find(slot->parent_) != processed_slots.end())
				continue;

			num_advertisements = slot->parent_->num_advertisements_;
			processed_slots.insert(slot->parent_);
		}

		//ignore overlay || pause || empty slot
		if (slot->time_position_class_ == "overlay" || slot->time_position_class_ == "pause_midroll")
		{
			ADS_DEBUG((LP_DEBUG, "bumper ad: not append bumper for overlay & pause, time position class %s.\n",
				slot->time_position_class_.c_str()));
			continue;
		}

		if (num_advertisements == 0 || non_empty_slots.find(slot->parent_ ? slot->parent_ : slot) == non_empty_slots.end())
		{
			ADS_DEBUG((LP_DEBUG, "bumper ad: not append bumper for empty slot.\n"));
			continue;
		}

		Ads_Advertisement_Candidate_Vector bumpers;
		// Only append end bumper for preroll & midroll slot
		if ((slot->time_position_class_ == "preroll" || slot->time_position_class_ == "midroll") && ctx->bumper_.second && slot->time_position_ != size_t(-1))
		{
			bumpers.push_back(ctx->bumper_.second);
		}
		else
		{
			ADS_DEBUG((LP_DEBUG, "bumper ad: not append end bumper for postroll slot %s, slot time position %d, time position class %s.\n",
				slot->custom_id_.c_str(),
				slot->time_position_,
				slot->time_position_class_.c_str()));
		}
		// Only append front bumper for
		// 1. midroll slot
		// 2. postroll slot
		// 3. preroll slot which has content view before it
		bool has_content_view_before_slot = content_duration > 0 || slot->time_position_ > 0;
		bool enable_preroll_front_bumper = has_content_view_before_slot || ctx->enable_intro_bumper();
		if ((enable_preroll_front_bumper || slot->time_position_class_ == "postroll" || slot->time_position_class_ == "midroll") && ctx->bumper_.first && slot->time_position_ != size_t(-1))
		{
			bumpers.push_back(ctx->bumper_.first);
		}
		else
		{
			ADS_DEBUG((LP_DEBUG, "bumper ad: not append front bumper for preroll slot %s because doesn't have content view before it, session content view duration %d, slot time position %d, time position class %s.\n",
				slot->custom_id_.c_str(),
				content_duration,
				slot->time_position_,
				slot->time_position_class_.c_str()));
		}

		for (Ads_Advertisement_Candidate_Vector::iterator bit = bumpers.begin(); bit != bumpers.end(); ++bit)
		{
			Ads_Advertisement_Candidate *candidate = *bit;
			// For mitosis, only append front bumper for the first sub slot & only append end bumper for the last sub slot.
			if (slot->parent_)
			{
				if (candidate == ctx->bumper_.second)
					slot = reinterpret_cast<Ads_Video_Slot *>(slot->parent_->children_.back());
				else
					slot = reinterpret_cast<Ads_Video_Slot *>(slot->parent_->children_.front());
			}

			Creative_Rendition_List& creatives = candidate->applicable_creatives(slot);

			for (Creative_Rendition_List::const_iterator it = candidate->creatives_.begin(); it != candidate->creatives_.end(); ++it)
			{
				const Ads_Creative_Rendition *creative = *it;
				if (!this->is_creative_applicable_for_slot(ctx, candidate, creative, slot, true /* ignore slot restriction */))
					continue;
				creatives.push_back(creative);
			}

			if (creatives.empty())
			{
				ADS_DEBUG((LP_DEBUG, "bumper ad: no applicable creative found for ad %s in slot %s.\n",
					ADS_ENTITY_ID_CSTR(candidate->ad_->id_),
					slot->custom_id_.c_str()));
				continue;
			}

			Ads_Advertisement_Candidate_Ref *ref = new Ads_Advertisement_Candidate_Ref(candidate, slot);
			ref->replica_id_ = candidate->references_.size();

			Ads_Selector::choose_creative_rendition_for_advertisement(ctx, candidate, slot, ref->creative_, candidate->ad_->id_, candidate->applicable_creatives(slot));
			candidate->used_slots_.insert(ref->slot_);
			candidate->references_.push_back(ref);

			this->calculate_effective_inventory(ctx, ref);
			//candidate_refs.push_back(ref);
			this->take_advertisement_candidate_reference(ctx, ref, candidate_refs);

			ADS_DEBUG((LP_TRACE, "bumper ad: append bumper %s creative %s in slot %s\n",
				ADS_ENTITY_ID_CSTR(candidate->ad_->id_),
				ADS_ENTITY_ID_CSTR(ref->creative_->id_),
				slot->custom_id_.c_str()));
		}
	}

	return 0;
}

void
Ads_Video_Slot::take_advertisement(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate *candidate, bool mask_only)
{
	if (candidate->is_proposal()) return; // proposal ads doesn't take inventory

	if (!mask_only)
	{
		//this->duration_remain_ -= std::min (this->duration_remain_, candidate->duration(this));
		++this->num_advertisements_;
		this->advertisements_.insert(candidate);

		if (this->parent_)
		{
			//this->parent_->duration_remain_ -= std::min(this->parent_->duration_remain_, candidate->duration(this));
			++this->parent_->num_advertisements_;
			this->parent_->advertisements_.insert(candidate);
		}

		// arrange the order of candidates
		std::list<Ads_Advertisement_Candidate *>::iterator pos = this->ordered_advertisements_.end();

		// taking follower pod ad
		if (candidate->is_follower_pod_ad())
		{
			// search the correct position for the follower pod ad
			pos = std::find_if(this->ordered_advertisements_.begin(), this->ordered_advertisements_.end(),
					[candidate] (Ads_Advertisement_Candidate* filled_candidate) {
						return filled_candidate->get_leader_pod_ad() == candidate->get_leader_pod_ad()
							&& filled_candidate->get_next_pod_ad() == candidate;
					});
			ADS_ASSERT(pos != this->ordered_advertisements_.end());
			if (pos != this->ordered_advertisements_.end())
				++pos;
		}
		else
		{
			this->find_position_for_advertisement(ctx, candidate, pos, true);
		}

		const Ads_Creative_Rendition *creative = 0;

		if (Ads_Selector::choose_creative_rendition_for_advertisement(ctx, candidate, this, creative, candidate->ad_->id_, candidate->applicable_creatives(this)) < 0 || !creative)
		{
			return;
		}

		this->duration_remain_ -= this->duration_remain_ >= creative->duration_ ? creative->duration_ : this->duration_remain_;
		if (this->parent_) this->parent_->duration_remain_ -= this->parent_->duration_remain_ >= creative->duration_ ? creative->duration_ : this->parent_->duration_remain_;

		this->ordered_advertisements_.insert(pos, candidate);

		// slot => creative map may be incorrect for scheduled ad. It is not needed by scheduled ad.
		candidate->creative(this, creative);

		if (candidate->ad_->exclusivity_scope_ == Ads_Advertisement::ADJACENT_ADS || !candidate->ad_->creative_ad_ids_.empty()
			|| creative->clearcast_info().flag_ & Ads_Creative_Rendition::NOT_FIRST || creative->clearcast_info().flag_ & Ads_Creative_Rendition::NOT_LAST)
		{
			this->flags_ |= Ads_Slot_Base::FLAG_ADJACENT_EXCLUSIVE;
			if (this->parent_) this->parent_->flags_ |= Ads_Slot_Base::FLAG_ADJACENT_EXCLUSIVE;
		}

		for (auto* watcher : watchers_)
		{
			watcher->on_ad_filled_into_slot(*this, *candidate, *creative);
		}	
	}

	const Ads_Ad_Unit *ad_unit = candidate->ad_unit();
	if (ad_unit && ad_unit->position_in_slot_)
	{
		uint64_t mask = 0;
		if (ad_unit->position_in_slot_ == -1)
			mask |= Ads_Video_Slot::MASK_LAST_POSITION;
		else if (ad_unit->position_in_slot_ > 0 && ad_unit->position_in_slot_ <= Ads_Video_Slot::MAX_POSITION)
		{
			for (int i = ad_unit->position_in_slot_; i > 0; --i)
			{
				if (!(this->masks_ & (Ads_Video_Slot::MASK_FIRST_POSITION << (i - 1))))
				{
					mask |= (Ads_Video_Slot::MASK_FIRST_POSITION << (i - 1));
					break;
				}
			}
		}

		this->masks_ |= mask;
		candidate->slot_masks_[this] |= mask;
	}

	if (!mask_only && candidate->ad_->is_ad_chooser() && candidate->ad_->placement_)
	{
		if (!ads::entity::is_valid_id(ctx->sponsoring_ad_bundle_))
			ctx->sponsoring_ad_bundle_ = candidate->ad_->placement_id();
		else
		{
			ADS_DEBUG((LP_ERROR, "Multiple placments w ad chooser selected!! (current sposoring %s, still taking %s)\n"
				, ADS_ENTITY_ID_CSTR(ctx->sponsoring_ad_bundle_), ADS_ENTITY_ID_CSTR(candidate->ad_->placement_id())));
			ctx->error(
			    ADS_ERROR_MULTIPLE_AD_CHOOSER_ALLOWED,
			    ads::Error_Info::SEVERITY_WARN,
			    ADS_ERROR_MULTIPLE_AD_CHOOSER_ALLOWED_S,
			    ADS_ENTITY_ID_CSTR(ctx->sponsoring_ad_bundle_),
				ADS_ENTITY_ID_STR(candidate->ad_->placement_id()));
		}
	}

	if (candidate->ad_->is_soi_sponsorship() && this->share_of_impression_ads_.find(candidate->ad_->id_) != this->share_of_impression_ads_.end())
	{
		this->share_of_impression_sum_ -= candidate->ad_->shared_percentage_;
		this->share_of_impression_ads_.erase(candidate->ad_->id_);
	}
}

/**************************
 *@description: Insert candidate into ordered_advertisements_
 *@para:
 *  candidate: The candidate to be inserted into ordered_advertisements_.
 *  creative: Creative of the candidate
 *  is_fixed_position: Indicate whether position_in_sub_slot is used as the exact insertion position.
 *  position_in_sub_slot: (input&output)
 *        When is_fixed_position = true, indicate the position that this candidate should be inserted into;
 *        When is_fixed_position = false, output the exact position that this candidate has been inserted into.
 *$return:
 *  0 successfully insert candidate, <0 error
 *  
 **************************/
int Ads_Video_Slot::take_scheduled_advertisement(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate *candidate, const Ads_Creative_Rendition *creative, bool is_fixed_position, int &position_in_sub_slot)
{
	if (ctx == nullptr || candidate == nullptr)
	{
		ADS_LOG((LP_ERROR, "ctx[%p] or candidate[%p] is nullptr\n", ctx, candidate));
		return -1;
	}

	if (candidate->is_proposal())
	{
		return 0; // proposal ads doesn't take inventory
	}

	if(creative == nullptr || candidate->ad_ == nullptr)
	{
		ADS_LOG((LP_ERROR, "creative[%p] or candidate->ad_ is nullptr\n", creative));
		return -1;
	}
	ADS_DEBUG((LP_DEBUG, "creative %s, duration %lu\n", ADS_ENTITY_ID_CSTR(creative->creative_id_), creative->duration_));

	++num_advertisements_;
	advertisements_.insert(candidate);

	if (parent_ != nullptr)
	{
		++parent_->num_advertisements_;
		parent_->advertisements_.insert(candidate);
	}

	std::list<Ads_Advertisement_Candidate *>::iterator pos = ordered_advertisements_.end();
	if(!is_fixed_position)
	{
		position_in_sub_slot = ordered_advertisements_.size();
		if (has_last_in_slot_ad_filled_)
		{
			if(position_in_sub_slot == 0)
			{
				ADS_LOG((LP_ERROR, "ordered_advertisements_.size() is zero when there should be at least LIB ad.\n"));
				return -1;
			}
			--pos;     // non last in slot ad is filled BEFORE the last in slot ad
			--position_in_sub_slot;
		}
	}
	else
	{
		if(position_in_sub_slot == Ads_Advertisement_Candidate_Ref::LAST_POSITION) // last ad in sub slot
		{
			position_in_sub_slot = ordered_advertisements_.size();
		}
		if((size_t)position_in_sub_slot > ordered_advertisements_.size())
		{
			ADS_LOG((LP_ERROR, "Ad[%s] got wrong position[%d] in opportunity[%s].\n", ADS_ENTITY_ID_CSTR(candidate->ad_->id_), position_in_sub_slot, custom_id_.c_str()));
			return -1;
		}
		int candidate_index = 0;
		for(auto candidate_iter = ordered_advertisements_.begin(); candidate_iter != ordered_advertisements_.end(); ++candidate_iter, ++candidate_index)
		{
			if(candidate_index == position_in_sub_slot)
			{
				pos = candidate_iter;
				break;
			}
		}
	}

	duration_remain_ -=
			duration_remain_ >= creative->duration_ ? creative->duration_ : duration_remain_;
	if (parent_ != nullptr)
	{
		parent_->duration_remain_ -=
				parent_->duration_remain_ >= creative->duration_ ? creative->duration_ : parent_->duration_remain_;
	}

	ordered_advertisements_.insert(pos, candidate);

	if (candidate->ad_->exclusivity_scope_ == Ads_Advertisement::ADJACENT_ADS
			|| !candidate->ad_->creative_ad_ids_.empty()
			|| creative->clearcast_info().flag_ & Ads_Creative_Rendition::NOT_FIRST
			|| creative->clearcast_info().flag_ & Ads_Creative_Rendition::NOT_LAST)
	{
		flags_ |= Ads_Slot_Base::FLAG_ADJACENT_EXCLUSIVE;
		if (parent_ != nullptr)
		{
			parent_->flags_ |= Ads_Slot_Base::FLAG_ADJACENT_EXCLUSIVE;
		}
	}
	return 0;
}

void
Ads_Video_Slot::remove_advertisement(const Ads_Selection_Context *ctx, Ads_Advertisement_Candidate *candidate)
{
	if (candidate->is_proposal()) return; // proposal ads doesn't take inventory

	size_t creative_duration = candidate->creative_duration(this);
	this->duration_remain_ += creative_duration;
	--this->num_advertisements_;
	this->advertisements_.erase(candidate);

	if (this->parent_)
	{
		this->parent_->duration_remain_ += creative_duration;
		--this->parent_->num_advertisements_;
		this->parent_->advertisements_.erase(candidate);
	}

	std::list<Ads_Advertisement_Candidate *>::iterator rit = std::find(this->ordered_advertisements_.begin(), this->ordered_advertisements_.end(), candidate);
	if (rit != this->ordered_advertisements_.end())
		this->ordered_advertisements_.erase(rit);

	// BTW: leader is not replacable

	const Ads_Ad_Unit *ad_unit = candidate->ad_unit();
	if (ad_unit && ad_unit->position_in_slot_ != 0)
	{
		uint64_t mask = candidate->slot_masks_[this];
		this->masks_ &= ~mask;
		candidate->slot_masks_[this] = 0;
	}

	for (auto* watcher : watchers_)
	{
		watcher->on_ad_removed_from_slot(*this, *candidate, creative_duration);
	}	
}

/**************************
 *@description:
 *  Remove scheduled candidate from ordered_advertisements_.
 *  Maintain the slot attribute: advertisements_, num_advertisements_ etc.
 *@para:
 *  candidate: the candidate to be maintained
 *  position_in_slot: the candidate's position in ordered_advertisements_
 *  scheduled_repeatedly_in_sub_slot: if this candidate scheduled in sub_slot for more than once.
 *  scheduled_repeatedly_in_parent_slot: if this candidate scheduled in parent_slot for more than once.
 *@return:
 *  0 successfully remove candidate, <0 error
 *  
 **************************/
int Ads_Video_Slot::remove_scheduled_advertisement(Ads_Advertisement_Candidate *candidate, int position_in_sub_slot, size_t creative_duration, bool scheduled_repeatedly_in_sub_slot, bool scheduled_repeatedly_in_parent_slot)
{
	if (candidate == nullptr || candidate->ad_ == nullptr)
	{
		ADS_LOG((LP_ERROR, "candidate[%p] or candidate->ad_ is nullptr.\n", candidate));
		return -1;
	}
	if (position_in_sub_slot == Ads_Advertisement_Candidate_Ref::LAST_POSITION)
	{
		position_in_sub_slot = ordered_advertisements_.size() - 1;
	}
	if ((size_t)position_in_sub_slot >= ordered_advertisements_.size())
	{
		ADS_LOG((LP_ERROR, "Scheduled ad[%s] got wrong position[%d] in opportunity[%s]. Remove fail.\n", ADS_ENTITY_ID_CSTR(candidate->ad_->id_), position_in_sub_slot, this->custom_id_.c_str()));
		return -1;
	}

	int sc_index = 0;
	for (auto scit = ordered_advertisements_.begin(); scit != ordered_advertisements_.end(); ++scit, ++sc_index)
	{
		if (sc_index == position_in_sub_slot)
		{
			if (*scit != candidate)
			{
				ADS_LOG((LP_ERROR, "Position[%d] in opportunity[%s] is candidate[%p,%s], not candidate[%p,%s]. Remove fail.\n",
				     position_in_sub_slot, custom_id_.c_str(), (*scit), (*scit)->ad_ != nullptr ? ADS_ENTITY_ID_CSTR((*scit)->ad_->id_) : "nullptr", candidate, ADS_ENTITY_ID_CSTR(candidate->ad_->id_)));
				return -1;
			}
			scit = ordered_advertisements_.erase(scit);
			break;
		}
	}

	--num_advertisements_;
	if (parent_ != nullptr)
	{
		--parent_->num_advertisements_;
	}

	// Maintain the slot internal state slot->advertisements_.
	if (!scheduled_repeatedly_in_sub_slot) // The candidate is scheduled only once in the current slots, remove it
	{
		advertisements_.erase(candidate);
	}
	if (parent_ != nullptr && !scheduled_repeatedly_in_parent_slot)
	{
		parent_->advertisements_.erase(candidate);
	}

	duration_remain_ += creative_duration;
	if (parent_ != nullptr)
	{
		parent_->duration_remain_ += creative_duration;
	}

	// flag FLAG_ADJACENT_EXCLUSIVE cannot be recovered
	return 0;
}

bool
Ads_Video_Slot::find_position_for_advertisement(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate *candidate, std::list<Ads_Advertisement_Candidate *>::iterator &pos, bool verbose)
{
	bool recoverable = false;
	return find_position_for_advertisement(ctx, candidate, pos, verbose, recoverable);
}

bool
Ads_Video_Slot::find_position_for_advertisement(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate *candidate, std::list<Ads_Advertisement_Candidate *>::iterator &pos, bool verbose, bool& recoverable)
{
	if (candidate == nullptr)
		return false;

	pos = this->ordered_advertisements_.end();

	// for dynamic ads
	recoverable = false;
#if defined(ADS_ENABLE_FORECAST)
	if (!(ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::BYPASS_RESTRICTION) && candidate->applicable_creatives(this).empty())
#else
	if (candidate->applicable_creatives(this).empty())
#endif
	{
		if (candidate->lite_->is_watched())
			ctx->log_watched_info(*candidate->lite_, this, "NO_CREATIVE");
		return false;
	}

	const Ads_Advertisement_Candidate *first = nullptr;
	const Ads_Advertisement_Candidate *last = nullptr;
	const Ads_Video_Slot *first_slot = nullptr;

	if (this->parent_ != nullptr)
	{
		bool found = false;
		for (Ads_Slot_List::iterator it = this->parent_->children_.begin(); it != this->parent_->children_.end(); ++it)
		{
			Ads_Video_Slot *slot = dynamic_cast<Ads_Video_Slot *>(*it);
			if (slot == this)
			{
				found = true;
				continue;
			}

			if (!slot->ordered_advertisements_.empty())
			{
				if (found)
				{
					last = slot->ordered_advertisements_.front();
					break;
				}
				else
				{
					first = slot->ordered_advertisements_.back();
					first_slot = slot;
				}
			}
		}
	}

	const Ads_Advertisement_Candidate *prev = nullptr, *next = last;
	const Ads_Video_Slot *pre_slot = nullptr, *next_slot = this , *current_slot = this;
	bool satisfied = false, position_available = false, sort_position_by_score = true;
	int current_position_in_slot = int(this->ordered_advertisements_.size());
	int last_ad_position_in_slot = current_position_in_slot;
	std::list<Ads_Advertisement_Candidate *>::reverse_iterator rit = this->ordered_advertisements_.rbegin();

	bool slot_shuffle = ctx->cro_enable_slot_shuffle_ && candidate->references_.size() > 0;
	std::vector<std::list<Ads_Advertisement_Candidate *>::iterator> pos_candidates;

	std::vector<selection::Error_Code> tmp_errors;

#define ADJACENT_EXCLUSIVITY_ERROR_CODE(er) ((er).error_code(selection::Error_Code::ADJACENT_EXCLUSIVITY))

	while (true)
	{
		pos = rit.base();
		prev = (rit == this->ordered_advertisements_.rend() ? first : *rit);
		pre_slot = prev == first ? first_slot : this;

		Ad_Excluded_Reason excluded_reason;

		// not break pod ads
		if (next && next->is_follower_pod_ad())
			goto previous_position;

		if (prev != nullptr)
		{
			if (!candidate->test_adjacent_exclusivity(ctx, prev, current_slot, pre_slot, &excluded_reason))
			{
				tmp_errors.push_back(ADJACENT_EXCLUSIVITY_ERROR_CODE(excluded_reason));
				ctx->mkpl_executor_->log_ad_error(ctx, *candidate->lite_, selection::Error_Code::ADJACENT_EXCLUSIVITY, selection::Error_Code::COMPETITION_FAILURE);

				recoverable = true;
				goto previous_position;
			}
			if (prev != first)
			{
				// To reduce complexity and confusing, after discussion with PM, dynamic ads will ignore the PPT setting on scheduled ads
				if (prev->is_scheduled_in_slot(this))
				{
					if (has_last_in_slot_ad_filled_ && current_position_in_slot == last_ad_position_in_slot)
					{
						// PPT = -1 conflicts with last in slot ad
						if (candidate->ad_unit()->position_in_slot_ == -1)
						{
							satisfied = false;
							break;
						}
						else
						{
							goto previous_position;
						}
					}
					else
					{
						if (candidate->ad_unit()->position_in_slot_ > 0 && candidate->ad_unit()->position_in_slot_ <= current_position_in_slot)
						{	// not fill due to PPT
							satisfied = false;
							break;
						}
					}
				}
				else	 // prev is a dynamic ad
				{
					if (prev->ad_unit()->position_in_slot_ == candidate->ad_unit()->position_in_slot_)
					{
						if (prev->ad_->is_leader()) goto previous_position;
						else if (!candidate->ad_->is_leader())
						{
							bool is_unified_yield_participated_ad = ctx->is_unified_yield_participated_ad(candidate) || ctx->is_unified_yield_participated_ad(prev);
							if (!is_unified_yield_participated_ad && !slot_shuffle && sort_position_by_score && Ads_Selector::greater_advertisement_candidate(ctx, ads::MEDIA_VIDEO, candidate, prev, false /* priority_only */, false /* ignore_duration */, true /* ignore_active */))
							{
								position_available = true;
								goto previous_position;
							}

							// sort by id for QA
							if (ctx->request_flags() & Ads_Request::Smart_Rep::DONT_SHUFFLE_CANDIDATES)
							{
								if (!is_unified_yield_participated_ad && !slot_shuffle && sort_position_by_score && !Ads_Selector::greater_advertisement_candidate(ctx, ads::MEDIA_VIDEO, prev, candidate, false /* priority_only */, false /* ignore_duration */, true /* ignore_active */)
									&& candidate->ad_->id_ < prev->ad_->id_)
								{
									position_available = true;
									goto previous_position;
								}
							}
						}
					}
					if (candidate->ad_unit()->position_in_slot_ > 0)
					{
						if(prev->ad_unit()->position_in_slot_ <= 0 || candidate->ad_unit()->position_in_slot_ < prev->ad_unit()->position_in_slot_)
							goto previous_position;
					}
					else if (candidate->ad_unit()->position_in_slot_ < 0)
					{
						if (prev->ad_unit()->position_in_slot_ < 0 && candidate->ad_unit()->position_in_slot_ < prev->ad_unit()->position_in_slot_)
							goto previous_position;
					}
					else // currrent->ad_unit()->position_in_slot_ == 0
					{
						if (prev->ad_unit()->position_in_slot_ < 0)
							goto previous_position;
					}
				}
			}
		}
		else /* first ad */
#if defined(ADS_ENABLE_FORECAST)
            if (!(ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::BYPASS_RESTRICTION))
#endif
		{
			Creative_Rendition_List temp;

			for (Creative_Rendition_List::const_iterator it = candidate->applicable_creatives(this).begin();
				 it != candidate->applicable_creatives(this).end(); ++it)
			{
				const Ads_Creative_Rendition *rendition = *it;
				if (rendition->clearcast_info().flag_ & Ads_Creative_Rendition::NOT_FIRST)
				{
					continue;
				}
				temp.push_back(rendition);
			}

			if (temp.empty())
			{
				tmp_errors.push_back(selection::Error_Code::CLEARCAST_NO_APPLICABLE_POSITION_IN_SLOT);
				ctx->mkpl_executor_->log_ad_error(ctx, *candidate->lite_, selection::Error_Code::CLEARCAST_NO_APPLICABLE_POSITION_IN_SLOT, selection::Error_Code::COMPETITION_FAILURE);
				goto previous_position;
			}
			candidate->applicable_creatives(this).swap(temp);
		}

		if (next)
		{
			if (!candidate->test_adjacent_exclusivity_with_next_filled_candidate(ctx, next, current_slot , next_slot, &excluded_reason))
			{
				tmp_errors.push_back(ADJACENT_EXCLUSIVITY_ERROR_CODE(excluded_reason));
				ctx->mkpl_executor_->log_ad_error(ctx, *candidate->lite_, selection::Error_Code::ADJACENT_EXCLUSIVITY, selection::Error_Code::COMPETITION_FAILURE);

				recoverable = true;
				goto previous_position;
			}
		}
		else /* last ad */
#if defined(ADS_ENABLE_FORECAST)
            if (!(ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::BYPASS_RESTRICTION))
#endif
		{
			Creative_Rendition_List temp;

			for (Creative_Rendition_List::const_iterator it = candidate->applicable_creatives(this).begin();
				 it != candidate->applicable_creatives(this).end(); ++it)
			{
				const Ads_Creative_Rendition *rendition = *it;
				if (rendition->clearcast_info().flag_ & Ads_Creative_Rendition::NOT_LAST)
				{
					continue;
				}
				temp.push_back(rendition);
			}

			if (temp.empty())
			{
				tmp_errors.push_back(selection::Error_Code::CLEARCAST_NO_APPLICABLE_POSITION_IN_SLOT);
				goto previous_position;
			}
			else
				candidate->applicable_creatives(this).swap(temp);
		}

		// sort by id for QA
		if ((ctx->request_flags() & Ads_Request::Smart_Rep::DONT_SHUFFLE_CANDIDATES) && this->is_tracking_only()
			&& prev != nullptr && !prev->is_scheduled_in_slot(this) && candidate->ad_->id_ < prev->ad_->id_)
			goto previous_position;

		satisfied = true;

		if (slot_shuffle)
		{
			pos_candidates.emplace_back(pos);
			goto previous_position;
		}
		break;

previous_position:
		bool no_previous_pos = false;
		if (prev != nullptr)
		{
			if (prev->is_scheduled_in_slot(this))
			{
				if (prev != first) //no impact if the schedule ad in preslot
				{
					if (!has_last_in_slot_ad_filled_ || current_position_in_slot != last_ad_position_in_slot)
					{
						no_previous_pos = true; //dynamic ad can't be filled in front of normal schedule ad, except last in break ad
					}
				}
			}
			else if (prev->ad_unit()->position_in_slot_> 0 && prev->ad_unit()->position_in_slot_ <= current_position_in_slot)
			{
				no_previous_pos = true;	//prev is dynamic ad, need respect its ppt setting; fix MRM-27606
			}
		}

		--current_position_in_slot;

		if (rit == this->ordered_advertisements_.rend())
			no_previous_pos = true;

		if (no_previous_pos)
		{
			if (sort_position_by_score && position_available)
			{
				current_position_in_slot = int(this->ordered_advertisements_.size());
				sort_position_by_score = false;
				position_available = false;
				rit = this->ordered_advertisements_.rbegin();
				next = last;
				continue;
			}
			break;
		}
		else
		{
			++rit;
			next = prev;
		}
	}

	if (slot_shuffle)
	{
		satisfied = !pos_candidates.empty();
		size_t random_val = ctx->rand();
		pos = satisfied ? pos_candidates[random_val % pos_candidates.size()] : rit.base();
	}

	if (!satisfied)
	{
		if (verbose)
			ADS_DEBUG0((LP_TRACE, "failed to find an applicable position for ad %s in slot %s\n", CANDIDATE_LITE_ID_CSTR(candidate->lite_), this->custom_id_.c_str()));
		if (candidate->lite_->is_watched())
			ctx->log_watched_info(*candidate->lite_, this, "NO_APPLICABLE_POSITION");

		for (auto error : tmp_errors)
		{
			uint32_t reject_reason = 0;
			if (ctx->decision_info_.reject_ads_.get_reject_reason(error, reject_reason) > 0)
				candidate->lite_->set_sub_reject_reason_bitmap(reject_reason);

			candidate->lite_->log_selection_error(error, this, selection::Error_Code::COMPETITION_FAILURE);
		}
	}
	else
		recoverable = false;

	return satisfied;

#undef ADJACENT_EXCLUSIVITY_ERROR_CODE
}

int
Ads_Selector::find_leader_for_follower(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate* candidate, const Ads_Advertisement_Candidate*& leader, Ads_Slot_List& slots, Ads_Slot_Base *slot, std::map<Ads_Slot_Base *, const Ads_Advertisement_Candidate *>* companion_leaders)
{
	leader = 0;
	const Ads_Advertisement *ad = candidate->ad_;

	Ads_Slot_List::iterator it = std::find(slots.begin(), slots.end(), slot);
	ADS_ASSERT(it !=  slots.end());

	--it;

	const Ads_Slot_Base *previous_slot = 0;
	for (; it >= slots.begin(); --it)
	{
		Ads_Slot_Base *aslot = *it;
		Ads_Video_Slot* vslot = reinterpret_cast<Ads_Video_Slot *>(aslot);
		ADS_ASSERT(vslot);

		if (vslot->time_position_class_ == "overlay" || vslot->time_position_class_ == "pause_midroll") continue;

		if (previous_slot && previous_slot != vslot && previous_slot != vslot->parent_) break;

		if (companion_leaders)
		{
			std::map<Ads_Slot_Base *, const Ads_Advertisement_Candidate *>::const_iterator lit = companion_leaders->find(vslot->parent_ ? vslot->parent_ : vslot);
			if (lit != companion_leaders->end())
				leader = lit->second;
		}
		else
		{
			{
				for (Ads_Advertisement_Candidate_Set::const_iterator ait = vslot->advertisements_.begin();
					 ait != vslot->advertisements_.end(); ++ait)
				{
					const Ads_Advertisement_Candidate *acandidate = (*ait);

					if (acandidate->ad_->placement_id() == ad->placement_id())
					{
						leader = acandidate;
						break;
					}
				}
			}
		}
		if (leader) break;

		previous_slot = (vslot->parent_ ? vslot->parent_ : vslot);
	}

	return leader ? 0 : -1;
}

int
Ads_Selector::take_advertisement_candidate_reference(Ads_Selection_Context * ctx, Ads_Advertisement_Candidate_Ref *ref, Ads_Advertisement_Candidate_Ref_List &refs,
		Take_Advertisement_Slot_Position* slot_position, Take_Advertisement_Candidate_Rank* candidate_rank)
{
#if defined(ADS_ENABLE_FORECAST)
	if (ctx->request_flags() & Ads_Request::Smart_Rep::FORCE_PROPOSAL) // no sort for UGA/GA
	{
		refs.insert(refs.begin(), ref);
		return 0;
	}
#endif

	Ads_Advertisement_Candidate_Ref_List::iterator it = refs.begin();
#if defined(ADS_ENABLE_FORECAST)
	bool find_slot_in_cache = false;
	if (slot_position)
	{
		const auto& it_slot = slot_position->find(ref->slot_);
		if (it_slot != slot_position->end())
		{
			find_slot_in_cache = true;
			it = it_slot->second;
		}
	}
#endif

	while (it != refs.end())
	{
		Ads_Advertisement_Candidate_Ref *aref = *it;

		if (aref->slot_ != ref->slot_)
		{
#if defined(ADS_ENABLE_FORECAST)
			if (find_slot_in_cache)
				break;
#endif
			if (Ads_Slot_Base::compare_for_slot(ref->slot_, aref->slot_, ctx->request_flags() & Ads_Request::Smart_Rep::DONT_SHUFFLE_CANDIDATES))
				break;
		}
		else
		{
			if (aref->candidate_ == ref->candidate_)
			{
				// refs which are not last in break should be put ahead of last in break ref
				if (aref->is_last_in_slot())
					break;
				const Ads_Slot_Base *slot = 0, *aslot = 0;
				for (Ads_Advertisement_Candidate_Ref_Vector::const_iterator sit = ref->companion_candidates_.begin(); sit != ref->companion_candidates_.end(); ++sit)
				{
					if ((*sit)->candidate_->ad_unit()->is_temporal())
					{
						slot = (*sit)->slot_;
						break;
					}
				}

				for (Ads_Advertisement_Candidate_Ref_Vector::const_iterator sit = aref->companion_candidates_.begin(); sit != aref->companion_candidates_.end(); ++sit)
				{
					if ((*sit)->candidate_->ad_unit()->is_temporal())
					{
						aslot = (*sit)->slot_;
						break;
					}
				}

				//ADS_ASSERT(slot && aslot);
				//we need to support one ad duplicated in a slot and keep their sequence.
				//if (!slot || !aslot) break;

				if (slot != aslot && slot && aslot)
				{
					if (Ads_Slot_Base::compare_for_slot(slot, aslot, ctx->request_flags() & Ads_Request::Smart_Rep::DONT_SHUFFLE_CANDIDATES)) break;
				}
				else
				{
					if (ref->replica_id_ < aref->replica_id_) break;
				}
				goto next;
			}

			// fallback
			if (aref->is_fallback_) break;
			else if (ref->is_fallback_) goto next;

			// companion
			if (aref->is_companion_) break;
			else if (ref->is_companion_) goto next;

			// bumper
			if (ctx->bumper_.first)
			{
				if (ctx->bumper_.first == ref->candidate_) break;
				else if (ctx->bumper_.first == aref->candidate_) goto next;
			}

			if (ctx->bumper_.second)
			{
				if (ctx->bumper_.second == aref->candidate_) break;
				else if (ctx->bumper_.second == ref->candidate_) goto next;
			}

			// variable
			if (aref->candidate_->ad_->is_variable()) break;
			else if (ref->candidate_->ad_->is_variable())
			{
				// variable ad should be put in front of last in break ad
				if (aref->is_last_in_slot())
					break;
				else
					goto next;
			}

			if (ref->slot_->env() == ads::ENV_VIDEO)
			{
				// normal video ads should be put in front of last in break ad
				if (aref->is_last_in_slot())
					break;

				if (aref->is_scheduled_delivered())
					goto next;
				// dynamic replacer behaves like a scheduled ad
				if (aref->is_replacement())
					goto next;

				Ads_Video_Slot *vslot = reinterpret_cast<Ads_Video_Slot *>(ref->slot_);
				bool found = false;
#if defined(ADS_ENABLE_FORECAST)
				if (ref->candidate_->is_proposal() || aref->candidate_->is_proposal())
				{
					bool is_greater = false, find_in_candidate_rank = false;
					if (candidate_rank)
					{
						const auto& x = candidate_rank->find(ref->candidate_);
						const auto& y = candidate_rank->find(aref->candidate_);
						if (x != candidate_rank->end() && y != candidate_rank->end())
						{
							find_in_candidate_rank = true;
							is_greater = x->second > y->second;
						}
					}
					if (!find_in_candidate_rank) // fallback for edge case
						is_greater = Ads_Selector::greater_advertisement_candidate(ctx, ads::MEDIA_VIDEO, ref->candidate_, aref->candidate_, false /* priority_only */, false /* ignore_duration */, true /* ignore_active */);
					if (is_greater)
						break;
					goto next;
				}
#endif

				for (std::list<Ads_Advertisement_Candidate *>::iterator it = vslot->ordered_advertisements_.begin();
					 it != vslot->ordered_advertisements_.end(); ++it)
				{
					if (ref->candidate_ == *it) { found = true; break; }
					if (aref->candidate_ == *it) break;
				}
				if (found) break;
			}
		}
next:
		++it;
	}

#if defined(ADS_ENABLE_FORECAST)
	Ads_Advertisement_Candidate_Ref_List::iterator new_it = refs.insert(it, ref);
	if (slot_position && (!find_slot_in_cache || it == (*slot_position)[ref->slot_]))
		(*slot_position)[ref->slot_] = new_it;
#else
	refs.insert(it, ref);
#endif

	return 0;
}

int
Ads_Selector::calculate_advertisement_position_in_slot(Ads_Selection_Context * ctx, Ads_Advertisement_Candidate_Ref_List &refs)
{
	int pos = 0;
	const Ads_Slot_Base *previous_slot = 0;
	for (Ads_Advertisement_Candidate_Ref_List::iterator it = refs.begin(); it != refs.end(); ++it)
	{
		Ads_Advertisement_Candidate_Ref *ref = *it;

		if (ref->slot_->env() != ads::ENV_VIDEO) continue;
		Ads_Slot_Base *slot = (reinterpret_cast<const Ads_Video_Slot *>(ref->slot_)->parent_ ? reinterpret_cast<const Ads_Video_Slot *>(ref->slot_)->parent_ : ref->slot_);

		if (ref->is_companion_) continue;
		if (ref->is_fallback_) continue;
		if (ref->candidate_->ad_->is_bumper()) continue;

		/// reset pos when entering new parent slot
		if (previous_slot != slot)
		{
			pos = 0;
			previous_slot = slot;
		}

		ref->position_in_slot_ = pos;

		if (!ref->fallback_candidates_.empty())
		{
			for (Ads_Advertisement_Candidate_Ref_List::iterator fit = ref->fallback_candidates_.begin(); fit != ref->fallback_candidates_.end(); ++fit)
			{
				Ads_Advertisement_Candidate_Ref *fallback = *fit;
				fallback->position_in_slot_ = ref->position_in_slot_;
			}
		}

#if defined(ADS_ENABLE_FORECAST)
		if (ctx->request_flags() & Ads_Request::Smart_Rep::FORCE_PROPOSAL)
			continue; // pos = 0 for the first 2 scenarios
		if (ref->candidate_->is_proposal()) continue;
#endif

		++pos;
	}
	return 0;
}

int
Ads_Selector::initialize_sorted_assets(Ads_Selection_Context *ctx, Ads_Asset_Section_Closure* closure, const Ads_GUID_Set& assets, Ads_GUID_Vector& asset_closure, ads::MEDIA_TYPE media_type)
{
    Ads_GUID_List base_assets;
    for (Ads_GUID_Set::const_iterator it = assets.begin(); it != assets.end(); ++it)
        if (!ads::asset::is_group(*it)) base_assets.push_back(*it);
    Ads_Selector::generate_sorted_asset_closure(ctx, ctx->repository_, ads::entity::restore_looped_id(closure->network_id_), base_assets, asset_closure, media_type);
    return 0;
}

int
Ads_Selector::get_rbb_prediction_model(Ads_Selection_Context *ctx, Ads_Asset_Section_Closure* closure)
{
	const Ads_Repository *repo = ctx->repository_;
	const Ads_Network& reseller = closure->network_;
	if (reseller.config() == nullptr)
		return -1;

	if (!reseller.config()->nielsen_ocr_api_enabled_ && !reseller.config()->comscore_vce_api_enabled_) return -1;
	if (ads::entity::is_valid_id(reseller.config()->rbp_proxy_network_id_) && ctx->closures_.find(reseller.config()->rbp_proxy_network_id_) != ctx->closures_.end())
	{
		get_rbb_prediction_model_helper(ctx->now_, repo, ctx->closures_[reseller.config()->rbp_proxy_network_id_], closure->prediction_model_);
	}
	else
		get_rbb_prediction_model_helper(ctx->now_, repo, closure, closure->prediction_model_);

    Ads_GUID cro_networks [] = {ctx->root_network_id(ads::MEDIA_VIDEO), ctx->root_network_id(ads::MEDIA_SITE_SECTION)};
    for (size_t i = 0;i < 2; ++i)
    {
        const Ads_Network* network = NULL;
        if (!ads::entity::is_valid_id(cro_networks[i]) || repo->find_network(cro_networks[i], network) < 0 || !network) continue;
        size_t flags = 0;
        if (network->get_rbp_reseller_config(&reseller, ctx->distributor_id_, flags) < 0) continue;
        closure->mutable_rbp_info().set_rbp_beacon_flags(flags);
        break;
    }
    return 0;
}

int
Ads_Selector::get_rbb_prediction_model_helper(time_t date, const Ads_Repository *repo, Ads_Asset_Section_Closure* closure, Ads_Repository::Prediction_Model_Value& prediction_model)
{
	prediction_model.clear();
	const Ads_Network &network = closure->network_;
	if (network.config() == nullptr)
		return -1;

	Ads_GUID nielsen_subtotal_id_key = ads::entity::make_id(ADS_ENTITY_TYPE_NIELSEN_DEMOGRAPHIC, -1);
	Ads_GUID comscore_subtotal_id_key = ads::entity::make_id(ADS_ENTITY_TYPE_COMSCORE_DEMOGRAPHIC, -1);

	bool nielsen_prediction_model_found = !network.config()->nielsen_ocr_api_enabled_;
	bool comscore_prediction_model_found = !network.config()->comscore_vce_api_enabled_;

	if (network.config()->rbp_custom_ingest_)
	{
		for (Ads_GUID_Vector::const_iterator a_it = closure->rbp_info().sorted_assets_.begin(); a_it != closure->rbp_info().sorted_assets_.end() && !nielsen_prediction_model_found; ++a_it)
			for (Ads_GUID_Vector::const_iterator s_it = closure->rbp_info().sorted_sections_.begin(); s_it != closure->rbp_info().sorted_sections_.end() && !nielsen_prediction_model_found; ++s_it)
			{
                Ads_Repository::Rbp_Custom_Ingest_Model::const_iterator it = repo->custom_prediction_model_.find(std::make_pair(*a_it, *s_it));
                if (it == repo->custom_prediction_model_.end()) continue;
				//Nielsen
				if (it->second.find(Ads_Advertisement::DATA_SOURCE_NIELSEN) != it->second.end())
				{
					Ads_Repository::Rbp_Custom_Ingest_Model_Data_Source::const_iterator source_it = it->second.find(Ads_Advertisement::DATA_SOURCE_NIELSEN);
					Ads_Repository::Rbp_Custom_Ingest_Model_Date_Value::const_iterator d_it =  source_it->second.lower_bound(std::make_pair(date, 0));
					if (d_it != source_it->second.end())
					{
						if (d_it->first.first <= date && (d_it->first.second <= 0 || date < d_it->first.second))
						{
							for (Ads_Repository::Prediction_Model_Value::const_iterator v_it = d_it->second.begin(); v_it != d_it->second.end(); ++v_it)
							{
								if ((ads::entity::type(v_it->first) == ADS_ENTITY_TYPE_NIELSEN_DEMOGRAPHIC && !nielsen_prediction_model_found))
									prediction_model[v_it->first] = v_it->second;
							}
							if (!nielsen_prediction_model_found && prediction_model.find(nielsen_subtotal_id_key) != prediction_model.end()) {
								ADS_DEBUG((LP_TRACE, "find Nielsen custom ingest model for closure %d, with settings on asset:%s, section:%s, start_date:%s, end_date:%s\n", ads::entity::id(closure->network_id_), ADS_ASSET_ID_CSTR(*a_it), ADS_ASSET_ID_CSTR(*s_it), ads::string_date(d_it->first.first).c_str(), ads::string_date(d_it->first.second).c_str()));
								nielsen_prediction_model_found = true;
							}
						}
					}
				}
			}
        for (Ads_GUID_Vector::const_iterator a_it = closure->rbp_info().sorted_assets_.begin(); a_it != closure->rbp_info().sorted_assets_.end() && !comscore_prediction_model_found; ++a_it)
            for (Ads_GUID_Vector::const_iterator s_it = closure->rbp_info().sorted_sections_.begin(); s_it != closure->rbp_info().sorted_sections_.end() && !comscore_prediction_model_found; ++s_it)
            {
                Ads_Repository::Rbp_Custom_Ingest_Model::const_iterator it = repo->custom_prediction_model_.find(std::make_pair(*a_it, *s_it));
                if (it == repo->custom_prediction_model_.end()) continue;
				//Comscore
				if (it->second.find(Ads_Advertisement::DATA_SOURCE_COMSCORE) != it->second.end())
				{
					Ads_Repository::Rbp_Custom_Ingest_Model_Data_Source::const_iterator source_it = it->second.find(Ads_Advertisement::DATA_SOURCE_COMSCORE);
					Ads_Repository::Rbp_Custom_Ingest_Model_Date_Value::const_iterator d_it =  source_it->second.lower_bound(std::make_pair(date, 0));
					if (d_it != source_it->second.end())
					{
						if (d_it->first.first <= date && (d_it->first.second <= 0 || date < d_it->first.second))
						{
							for (Ads_Repository::Prediction_Model_Value::const_iterator v_it = d_it->second.begin(); v_it != d_it->second.end(); ++v_it)
							{
								if ((ads::entity::type(v_it->first) == ADS_ENTITY_TYPE_COMSCORE_DEMOGRAPHIC && !comscore_prediction_model_found))
									prediction_model[v_it->first] = v_it->second;
							}
							if (!comscore_prediction_model_found && prediction_model.find(comscore_subtotal_id_key) != prediction_model.end()) {
								ADS_DEBUG((LP_TRACE, "find comScore custom ingest model for closure %d, with settings on asset:%s, section:%s, start_date:%s, end_date:%s\n", ads::entity::id(closure->network_id_), ADS_ASSET_ID_CSTR(*a_it), ADS_ASSET_ID_CSTR(*s_it), ads::string_date(d_it->first.first).c_str(), ads::string_date(d_it->first.second).c_str()));
								comscore_prediction_model_found = true;
							}
						}
					}
				}
			}
		if (nielsen_prediction_model_found && comscore_prediction_model_found) {
			return 0;
		}
		if (network.config()->nielsen_ocr_api_enabled_ && !nielsen_prediction_model_found)
			ADS_DEBUG((LP_TRACE, "Could not find Nielsen custom ingest model for closure %s\n", ads::entity::str(closure->network_id_).c_str()));
		if (network.config()->comscore_vce_api_enabled_ && !comscore_prediction_model_found)
			ADS_DEBUG((LP_TRACE, "Could not find comScore custom ingest model for closure %s\n", ads::entity::str(closure->network_id_).c_str()));
    }
    // fallback to normal prediction model
    tm t;
    gmtime_r(&date, &t);
    for (Ads_GUID_Vector::const_iterator a_it = closure->rbp_info().sorted_assets_.begin(); a_it != closure->rbp_info().sorted_assets_.end(); ++a_it)
        for (Ads_GUID_Vector::const_iterator s_it = closure->rbp_info().sorted_sections_.begin(); s_it != closure->rbp_info().sorted_sections_.end(); ++s_it)
        {
            Ads_Repository::Prediction_Model::const_iterator p_it = repo->prediction_model_.find(std::make_pair(*a_it, *s_it));
            if (p_it == repo->prediction_model_.end()) continue;
            const Ads_Repository::Prediction_Model_Date_Value& date_value = p_it->second;

            for (Ads_Repository::Prediction_Model_Date_Value::const_iterator date_value_it = date_value.begin(); date_value_it != date_value.end(); ++date_value_it)
            {
            	if ((!nielsen_prediction_model_found && date_value_it->second.find(nielsen_subtotal_id_key) == date_value_it->second.end())
            		&& (!comscore_prediction_model_found && date_value_it->second.find(comscore_subtotal_id_key) == date_value_it->second.end()))
            	{
            		continue;
            	}
            	for (Ads_Repository::Prediction_Model_Value::const_iterator v_it = date_value_it->second.begin(); v_it != date_value_it->second.end(); ++v_it)
	        	{
	        		if ((ads::entity::type(v_it->first) == ADS_ENTITY_TYPE_NIELSEN_DEMOGRAPHIC && !nielsen_prediction_model_found)
	        			|| (ads::entity::type(v_it->first) == ADS_ENTITY_TYPE_COMSCORE_DEMOGRAPHIC && !comscore_prediction_model_found))
	        		{
	        			prediction_model[v_it->first] = v_it->second;
	        		}
	        	}
	        	if (!nielsen_prediction_model_found && prediction_model.find(nielsen_subtotal_id_key) != prediction_model.end()) {
	            	ADS_DEBUG((LP_TRACE, "find Nielsen original prediction model for closure %d, with settings on asset:%s, section:%s\n", ads::entity::id(closure->network_id_), ADS_ASSET_ID_CSTR(*a_it), ADS_ASSET_ID_CSTR(*s_it)));
	            	nielsen_prediction_model_found = true;
	            }
	            if (!comscore_prediction_model_found && prediction_model.find(comscore_subtotal_id_key) != prediction_model.end()) {
	            	ADS_DEBUG((LP_TRACE, "find comScore original prediction model for closure %d, with settings on asset:%s, section:%s\n", ads::entity::id(closure->network_id_), ADS_ASSET_ID_CSTR(*a_it), ADS_ASSET_ID_CSTR(*s_it)));
	            	comscore_prediction_model_found = true;
	            }
	            if (nielsen_prediction_model_found && comscore_prediction_model_found) {
	            	return 0;
	            }
            }
        }
    return -1;
}

/*
int
Ads_Selector::calculate_advertisement_budget_expense(Ads_Selection_Context* ctx, const Ads_Advertisement* ad, time_t time_future, double& fill_rate, int64_t &can_deliver, int64_t &delivered)
{
	const Ads_Repository *repo = ctx->repository_;

	fill_rate = 0;
	can_deliver = 0;

	// sponsorship with unlimited budget
	if (ad->is_sponsorship() && ad->placement_ && ad->placement_->budget_ < 0 && ad->impression_cap_ < 0)
	{
		can_deliver = ad->placement_->budget_;
        return 0;
	}

	// impression cap check
	if (ad->impression_cap_ >= 0)
	{
		int64_t imps = 0, expense = 0;
		time_t timestamp = 0;
		if (ctx->read_ad_counter(ad, imps, expense, timestamp) >= 0
			&& ad->impression_cap_ <= std::max(ad->delivered_impression_, imps))
		{
			ADS_DEBUG((LP_TRACE, "%s reach impression cap\n", ADS_ADVERTISEMENT_ID_CSTR(ad->id_)));

			fill_rate = 1.0;
			return 0;
		}
	}

	const Ads_Advertisement *adg = ad;
	if (ads::entity::is_valid_id(ad->budget_control_id_))
	{
		if (ctx->find_advertisement(ad->budget_control_id_, adg) < 0 || !adg)
		{
				ADS_DEBUG((LP_DEBUG, "failed to find budget ad %s failed for %s\n", ADS_ADVERTISEMENT_ID_CSTR(ad->budget_control_id_), ADS_ADVERTISEMENT_ID_CSTR(ad->id_)));
				fill_rate = 1.0;
				return 0;
		}
	}

	int64_t expense_count = -1, impression_count = -1;
	time_t timestamp = 0;
	if (adg && ctx->read_ad_counter(adg, impression_count, expense_count, timestamp) < 0)
	{
		ADS_DEBUG((LP_DEBUG, "failed to read counter for %s\n", ADS_ADVERTISEMENT_ID_CSTR(adg->id_)));

		fill_rate = 1.0;
		return 0;
	}

	Ads_Advertisement::BUDGET_CONTROL_TYPE budget_type = adg->budget_control_type_;
	if (budget_type == Ads_Advertisement::CURRENCY)
	{
		//Not support currency goal type
		delivered = 0;
		fill_rate = 1.0;
		return 0;
	}

	int64_t delivered_impression = adg->delivered_impression_;
	int64_t budget = adg->budget_, remaining_budget = adg->budget_;

	// FDB-3607 use absolute schedule for user-custom delivery pacing
	if (adg->delivered_budget_before_restart_ > 0 && ad->delivery_pace_ != Ads_Advertisement::USER_CUSTOM && ad->delivery_pace_ != Ads_Advertisement::PRE_DEFINED)
		remaining_budget -= adg->delivered_budget_before_restart_;

	if (remaining_budget <= 0 && ! ads::entity::is_valid_id(ad->external_network_id_))
	{
		ADS_DEBUG((LP_TRACE, "%s no remaining budget\n", ADS_ADVERTISEMENT_ID_CSTR(ad->id_)));

		fill_rate = 1.0;
		return 0;
	}

	int64_t current_expense, current_expense_counter, unit_expense;
	current_expense_counter = impression_count;
	unit_expense = 1;

	current_expense = delivered_impression;
	if (current_expense < current_expense_counter)
	{
		current_expense = current_expense_counter;
	}

	delivered = current_expense;

	// budget check
	if (budget > 0 && budget < current_expense + unit_expense)
	{
		ADS_DEBUG((LP_TRACE, "%s reach budget\n", ADS_ADVERTISEMENT_ID_CSTR(adg->id_)));

		fill_rate = 1.0;
		return 0;
	}

	double progress = std::max(0.0, double(current_expense + remaining_budget - budget) / remaining_budget);
	//delivered = int64_t(progress * budget);
	if (!ad->is_active(ctx->time(), ctx->enable_testing_placement()))
	{
		fill_rate = 0;
		can_deliver = 0;
		return 0;
	}

	bool daily_pacing = false;
	{
		const char *s = 0;
		s = repo->network_function("DAILY_DELIVERY_PACING", ad->network_id_);
		if (s && s[0] && ads::str_to_i64(s))
			daily_pacing = true;
	}
	double daily_progress = -1.0;

	if (daily_pacing && !ads::entity::is_valid_id(ad->external_network_id_))
	{
		int64_t imps = 0, expense = 0;
		time_t timestamp = 0;
		if (ctx->read_ad_counter(adg, imps, expense, timestamp, true) < 0
				||(expense == 0 && imps == 0))
		{
				//no history
				expense = adg->expense_;
				imps = adg->delivered_impression_;
		}

		int64_t daily_expense = budget_type == Ads_Advertisement::CURRENCY? current_expense - expense: current_expense - imps;
		daily_progress = std::max(0.0, double(daily_expense) / remaining_budget);
	}

	daily_pacing = (daily_progress >= 0);

	//budgeted but AFAP
    if (ad->delivery_pace_ == Ads_Advertisement::AS_FAST_AS_POSSIBLE || ad->is_sponsorship())
	{
		fill_rate = progress;
		can_deliver = budget;
		if (ad->impression_cap_ >= 0 && can_deliver > ad->impression_cap_)
			can_deliver = ad->impression_cap_;
		//delivered = current_expense;
        return 0;
	}

	double target = 1.0;
	double daily_target = 1.0;
	double target_future = 1.0;
	double daily_target_future = 1.0;
	double time = 0.0;
	double time_override = 0.0;
//	Ads_GUID curve_id = -1;
	const size_t ONE_DAY = 60 * 60 * 24;

	if (ad->start_date_ != 0 && ad->end_date_ != 0)
	{
		///FDB-3607 use absolute schedule for user-custom delivery pacing
		time_t start_date = ((ad->delivery_pace_ == Ads_Advertisement::USER_CUSTOM || ad->delivery_pace_ == Ads_Advertisement::PRE_DEFINED) ? ad->start_date_ : std::max(ad->start_date_, ad->time_activated_));
		size_t total = ad->end_date_ - start_date;
		size_t elapsed = ctx->time() - start_date;
		size_t elapsed_override = time_future - start_date;
		int tz_offset = 0;
		int daily_elapsed = 0;
		const Ads_Network * network = 0;
		//performance consideration: calclulate timezone shifting only when needed.
		if (daily_pacing || ad->delivery_pace_ == Ads_Advertisement::DAILY_CAPPED)
		{
			if (repo->find_network(ad->network_id_, network) >= 0 && network)
				tz_offset = repo->tzoffset(network->tz_name_) * 60;
			daily_elapsed = ctx->time() %ONE_DAY + tz_offset;
			if (daily_elapsed < 0) daily_elapsed = ONE_DAY + daily_elapsed;
			if (daily_elapsed > (int)ONE_DAY) daily_elapsed = daily_elapsed - ONE_DAY;
			ADS_DEBUG((LP_TRACE, "Timezone shift: %s hours passed for today in timezone %s\n", ads::i64_to_str(daily_elapsed/3600).c_str(), network->tz_name_.c_str()));
		}

		//align to local timezone for daily capped ad
		if (ad->delivery_pace_ == Ads_Advertisement::DAILY_CAPPED)
		{
			elapsed = ONE_DAY * int((elapsed + ONE_DAY - daily_elapsed) / ONE_DAY);
			elapsed_override = ONE_DAY * int((elapsed_override + ONE_DAY - daily_elapsed) / ONE_DAY);
		}
		time = (total > 0 ? std::min(double(elapsed) / total, 1.0) : 1.0);
		time_override = (total > 0 ? std::min(double(elapsed_override) / total, 1.0) : 1.0);

		const Ads_Control_Curve *curve = 0;

		bool has_custom_pacing_points =false;
		if (ad->delivery_pace_ == Ads_Advertisement::USER_CUSTOM)
		{
			//FDB-12750 Move custom pacing calculation into ADS
			has_custom_pacing_points = ad->get_custom_pacing_target(time, target) && ad->get_custom_pacing_target(time_override, target_future);
		}
		else if (ads::entity::is_valid_id(ad->delivery_curve_id_))
		{
			repo->find_curve(ad->delivery_curve_id_, curve);
		}
		else
		{
			if (total <= ONE_DAY + 60)
				curve = repo->system_config().schedule_ad_1day_;
			else if (total <= ONE_DAY * 2 + 60)
				curve = repo->system_config().schedule_ad_2day_;
			if (! curve)
				curve = repo->system_config().schedule_ad_;
		}

		if (!has_custom_pacing_points)
		{
			if (curve)
			{
	//			curve_id = curve->id_;
				target = curve->apply(time);
				target_future = curve->apply(time_override);
			}
			else
			{
				target = time;
				target_future = time_override;
			}
		}
		target = std::max(target, 0.0001);
		target_future = std::max(target_future, 0.0001);

		if (daily_pacing)
		{
#if defined(ADS_ENABLE_FORECAST)
			int start_of_day = std::max((int)(elapsed - ctx->time() % ONE_DAY), 0);
#else
			int start_of_day = std::max((int)(elapsed - daily_elapsed), 0);
#endif
			if (ad->delivery_pace_ == Ads_Advertisement::DAILY_CAPPED)
				start_of_day = std::max((int)(elapsed - ONE_DAY), 0);

			double start_time = (total > 0 ? double(start_of_day) / total : 1.0);

			if (has_custom_pacing_points)
			{
				//FDB-12750 Move custom pacing calculation into ADS
				double start_target =0.0, override_target = 0.0;
				ad->get_custom_pacing_target(start_time, start_target);
				daily_target = target - start_target;

				ad->get_custom_pacing_target(time_override, override_target);
				daily_target_future = override_target - start_target;
			}
			else if (curve)
			{
//				curve_id = curve->id_;
				daily_target = curve->apply(time) - curve->apply(start_time);
				daily_target_future = curve->apply(time_override) - curve->apply(start_time);
			}
			else
			{
				daily_target =  time - start_time;
			}

			daily_target = std::max(daily_target, 0.0001);
			daily_target_future = std::max(daily_target_future, 0.0001);
		}
		else
		{
			daily_target_future = target_future;
		}
	}//end of target calculation

	//const double fill_rate = std::min(progress / target, 2.0); //ceil to 200%
	fill_rate = progress / target;
	can_deliver = int64_t(std::max(target_future, daily_target_future) * budget);
	//delivered = int64_t(progress * budget);

	if (daily_progress >= 0 && daily_pacing)
	{
			fill_rate = std::min(daily_progress / daily_target, 1.0); //ceil to 100%
			ADS_DEBUG((LP_DEBUG, "ad %s daily fill rate calculation: progress %f target %f => fill rate %f).\n", ADS_ENTITY_ID_CSTR(ad->id_), daily_progress, daily_target, fill_rate));
	}

	// extended delivery?
//	Ads_GUID ext_curve_id = -1;
	double ext_target = -1.0;

	// extended delivery
	if (ads::entity::is_valid_id(ad->ext_delivery_curve_id_))
	{
		bool use = true;
		if (ctx->root_asset_ && ctx->root_asset_->network_id_ != ctx->network_id_)
		{
			if (ctx->root_asset_->network_id_ == ad->network_id_)
			{
				if (ctx->distributor_revenue_share_.first >= 0 && ad->ext_delivery_curve_precondition_ >= 0
				&& ADS_TO_REAL_PERCENTAGE(ctx->distributor_revenue_share_.first) * 100 > ad->ext_delivery_curve_precondition_)
					use = false;
			}
		}

		const Ads_Control_Curve *curve = 0;
		if (use)
			repo->find_curve(ad->ext_delivery_curve_id_, curve);

		if (curve)
		{
//			ext_curve_id = curve->id_;
			ext_target = curve->apply(time);
			fill_rate = std::min(progress / ext_target, 2.0); //ceil to 200%

			can_deliver = std::min(int64_t(ext_target * budget), budget * 2);
		}
	}

	return 0;
}
*/

int
Ads_Selector::generate_sorted_asset_closure(Ads_Selection_Context* ctx, const Ads_Repository* repo, const Ads_GUID network_id, const Ads_GUID_List& base_assets, Ads_GUID_Vector& asset_closure, ads::MEDIA_TYPE media_type)
{
	asset_closure.clear();
    const Ads_Network* network = NULL;
    if (repo->find_network(network_id, network) < 0 || !network) return -1;
#if !defined(ADS_ENABLE_FORECAST)
    if (!network->config()->nielsen_ocr_api_enabled_ && !network->config()->comscore_vce_api_enabled_) return -1;
#endif

	std::map<Ads_GUID, size_t> asset_parents;
	std::map<Ads_GUID, Ads_GUID_Set> asset_children;
    std::map<Ads_GUID, Ads_GUID_List> asset_meta_groups;
    Ads_GUID_Set lt_assets, series;

    Ads_GUID_List assets;
    assets.insert(assets.begin(), base_assets.begin(), base_assets.end());

    Ads_GUID_Set parents_set;
    	while (!assets.empty())
	{
		Ads_GUID asset_id = assets.front();
		assets.pop_front();

		if (parents_set.find(asset_id) != parents_set.end()) continue;
		parents_set.insert(asset_id);

		const Ads_Asset_Base *asset = 0;
        if (ctx && (ctx->find_asset_or_section(asset_id, asset) < 0 || !asset)) continue;
        if (!asset && (Ads_Repository::find_asset_or_section(repo, asset_id, asset) < 0 || !asset)) continue;
        if (!ads::asset::is_group(asset_id))
        {
            lt_assets.insert(asset_id);
			network->try_apply_group_rules(asset, asset_meta_groups[asset_id]);
            assets.insert(assets.end(), asset_meta_groups[asset_id].begin(), asset_meta_groups[asset_id].end());
        }
        {
			std::copy(asset->parents_.begin(), asset->parents_.end(), std::back_inserter(assets));
            if (asset->is_series_or_site()) series.insert(asset_id);
        }
	}

	//topological sorting
	for (Ads_GUID_Set::iterator ait = parents_set.begin(); ait != parents_set.end(); ++ait)
	{
		const Ads_Asset_Base *asset = 0;
        Ads_GUID asset_id = *ait;
		if (ctx && (ctx->find_asset_or_section(asset_id, asset) < 0 || !asset))
			continue;
        if (!asset && (Ads_Repository::find_asset_or_section(repo, asset_id, asset) < 0 || !asset)) continue;

		if (!ads::asset::is_group(asset_id))
            for (Ads_GUID_List::const_iterator mit = asset_meta_groups[asset_id].begin(); mit != asset_meta_groups[asset_id].end(); ++mit)
                if (parents_set.count(*mit) != 0 && asset_children[*mit].count(asset_id) == 0)
                {
                    asset_children[*mit].insert(asset_id);
                    ++asset_parents[asset_id];
                }

        for (Ads_GUID_RVector::const_iterator pit = asset->parents_.begin(); pit != asset->parents_.end(); ++pit)
            if (parents_set.count(*pit) != 0 && asset_children[*pit].count(asset_id) == 0)
            {
                asset_children[*pit].insert(asset_id);
                asset_parents[asset_id]++;
            }
		if (asset_parents[asset_id] == 0) assets.push_back(asset_id);
	}

	while (!assets.empty())
	{
		Ads_GUID asset_id = assets.front();
        if (lt_assets.find(asset_id) == lt_assets.end() && series.find(asset_id) == series.end()) // handle asset and seris at last
            asset_closure.push_back(asset_id);
		assets.pop_front();

		Ads_GUID_Set &children = asset_children[asset_id];
		for (Ads_GUID_Set::iterator cit = children.begin(); cit != children.end(); ++cit)
        {
            std::map<Ads_GUID, size_t>::iterator c_it = asset_parents.find(*cit);
            if (c_it == asset_parents.end()) continue; //processed
			if ((-- c_it->second) == 0)
            {
                assets.push_back(*cit);
                asset_parents.erase(c_it);
            }
        }
        if (assets.empty() && (asset_closure.size() + lt_assets.size() + series.size() != parents_set.size())) //break circle
        {
            Ads_GUID toppest_asset = -1;
            size_t parent_count = (size_t)-1;
            for (std::map<Ads_GUID, size_t>::const_iterator it = asset_parents.begin(); it != asset_parents.end(); ++it)
            {
                if (it->second > 0 && parent_count > it->second)
                {
                    parent_count = it->second;
                    toppest_asset = it->first;
                }
            }
            if (!ads::entity::is_valid_id(toppest_asset)) break;
            assets.push_back(toppest_asset);
            asset_parents.erase(toppest_asset);
        }
	}

    asset_closure.insert(asset_closure.end(), series.begin(), series.end());
    asset_closure.insert(asset_closure.end(), lt_assets.begin(), lt_assets.end());
    std::reverse(asset_closure.begin(), asset_closure.end());
	ADS_ASSERT(asset_closure.size() == parents_set.size());
    if (asset_closure.empty()) //
        asset_closure.push_back(network->run_of_network_id(media_type));

    return 0;
}

Ads_String
Ads_Selector::demographic_platform_to_string(const Ads_Repository::DEMOGRAPHIC_PLATFORM platform)
{
	switch(platform)
	{
	case Ads_Repository::DEMOGRAPHIC_PLATFORM_PC:
		return "PC";
	case Ads_Repository::DEMOGRAPHIC_PLATFORM_MOBILE:
		return "MOBILE";
	case Ads_Repository::DEMOGRAPHIC_PLATFORM_OTT:
		return "OTT";
	case Ads_Repository::DEMOGRAPHIC_PLATFORM_TOTAL:
		return "TDP";
	default:
		ADS_LOG((LP_ERROR, "convert invalid demographic platform to UNKNOWN %d\n", platform));
		return "UNKNOWN";
	}
}

bool Ads_Selector::test_candidate_targeting_criteria(Ads_Selection_Context &ctx, const Ads_Advertisement *ad, const std::set<Ads_Term::TYPE> *checked_term_types)
{
	bool all_terms_ignored;
	Ads_GUID_List path;
	Ads_GUID_List failed_path;

	if (ad == nullptr)
		return false;

	Ads_Asset_Section_Closure_Map::iterator cit = ctx.closures_.find(ad->network_id_);
	if (cit == ctx.closures_.end())
		return false;

	Ads_Asset_Section_Closure * closure = cit->second;
	if (!closure->is_active())
		return false;
	Ads_GUID_Set terms(closure->terms_);

	bool res = test_partial_targeting_criteria(ctx.repository_, ctx.delta_repository_, terms, ad->targeting_criteria_, checked_term_types, all_terms_ignored, path, &failed_path);

	return res && !all_terms_ignored;
}

bool Ads_Selector::is_data_visibility_banned(Ads_Selection_Context *ctx, const Ads_MRM_Rule_Path* path, const Ads_Advertisement_Candidate *candidate, const Ads_Slot_Base* slot)
{
	bool should_reactivate_rule = false;
	return is_data_visibility_banned(ctx, path, candidate, slot, should_reactivate_rule);
}

bool Ads_Selector::is_data_visibility_banned(Ads_Selection_Context *ctx, const Ads_MRM_Rule_Path* path, const Ads_Advertisement_Candidate *candidate, const Ads_Slot_Base* slot, bool &should_reactivate_rule)
{
	bool watch_rule = true;
	bool is_watched = ctx->is_watched(*candidate->lite_);
	if (should_reactivate_rule)
	{
		should_reactivate_rule = watch_rule = false;
	}

	if(!ctx->data_right_management_->is_data_right_enabled())
		return false;

	//used to test if a candidate is banned by data right visibility through a path. Path = NULL, means CRO's ad.
	if (ctx->data_right_management_->restrict_data_visibility_whitelist().empty())
	{
		ADS_DEBUG((LP_TRACE, "whitelist for data right visibility should not be empty(DRO itself must be in whitelist)\n"));
		return true;
	}

	const Ads_Term_Criteria* ad_criteria = candidate->ad_->targeting_criteria_;
	if (ad_criteria != nullptr)
	{
		if (ctx->data_right_management_->is_criteria_restricted(candidate->get_network_id(), ad_criteria))
		{
			ADS_DEBUG((LP_TRACE, "Data Right: ad %s FAILED due to targeting on GEO/KV/AUDIENCE/PLATFORM but network %ld is restricted from using data.\n",
					CANDIDATE_LITE_ID_CSTR(candidate->lite_),
					ads::entity::id(candidate->get_network_id())));
			if (is_watched) ctx->log_watched_info(*candidate->lite_, "AD_TARGETING_TYPE_RESTRICTED");
			return true;
		}
	}

	const Ads_Repository *repo = ctx->repository_;
	if (path != nullptr)
	{
		for (const auto &edge : path->edges_)
		{
			const Ads_Network* current_network = nullptr;
			const Ads_MRM_Rule* rule = edge.rule_;
			if (rule == nullptr) return true;

			const Ads_Network* downstream_network = nullptr;
			if (repo->find_network(edge.partner_id(), downstream_network) < 0 || !downstream_network) return true;
			if (repo->find_network(edge.network_id(), current_network) < 0 || !current_network) return true;

			Ads_Asset_Section_Closure_Map::iterator cit = ctx->closures_.find(edge.network_id());
			if (cit == ctx->closures_.end())
				return true;//impossible
			Ads_Asset_Section_Closure *current_closure = cit->second;
			//FIXME: this need to be fixed for n-plc
			/* if( whitelist.find(current_network->id_) != whitelist.end()) */
			/* {//whitelist */
				/* has_restricted_rule = true; */
			/* } */
			// has restricted rule
			const Ads_Term_Criteria* rule_criteria = rule->targeting_criteria_;
			if (rule_criteria != nullptr)
			{
				if (ctx->data_right_management_->is_criteria_restricted(current_closure->network_id_, rule_criteria) && ctx->data_right_management_->is_criteria_restricted(candidate->operation_owner().network_.id_, rule_criteria))
				{
					ADS_DEBUG((LP_TRACE, "Data Right: network %ld grant data right rules on GEO/KV/AUDIENCE/PLATFORM fields %s to network %ld.\n",
							ads::entity::id(current_network->id_),
							ADS_RULE_CSTR(rule),
							ads::entity::id(downstream_network->id_)));
					ADS_DEBUG((LP_TRACE, "Data Right: ad %s FAILED due to upstream network grant a data right rule on GEO/KV/AUDIENCE/PLATFORM fields.\n",
							CANDIDATE_LITE_ID_CSTR(candidate->lite_)));
					if (candidate && slot && watch_rule && is_watched)
						ctx->log_watched_info(*candidate->lite_, slot, "RULE_TARGETING_TYPE_RESTRICTED");
					should_reactivate_rule = true;
					return true;
				}
			}
		}
	}

	return false;
}

// if is_overlap_enough is true, only test whether item's flight window overlaps container's
bool
Ads_Selector::test_flight_window(const time_t &item_start_time, const time_t &item_end_time, const time_t &container_start_time, const time_t &container_end_time, bool is_overlap_enough)
{
	if (item_start_time < 0 || item_end_time < 0 || container_start_time < 0 || container_end_time < 0)
	{
		ADS_LOG((LP_WARNING, "start time or end time is negative value!\n"));
		return false;
	}
	if (item_end_time > 0 && item_end_time < container_start_time) return false;
	if (item_start_time > 0 && item_start_time > container_end_time) return false;

	if (is_overlap_enough) return true;

	return ((!item_start_time || (item_start_time > 0 && item_start_time <= container_start_time))
			&& (!item_end_time || (item_end_time > 0 && container_end_time <= item_end_time)));
}


int
Ads_Selector::fill_variable_ad(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate *candidate, size_t ad_duration, const Ads_Creative_Rendition *creative, Ads_Video_Slot *vslot, bool is_placeholder_ad)
{
	if (ctx == nullptr || candidate == nullptr || creative == nullptr || vslot == nullptr)
	{
		ADS_LOG((LP_ERROR, "NULL ptr met. [%p/%p/%p/%p]\n", ctx, candidate, creative, vslot));
		return -1;
	}

	size_t ref_flags = 0;
	if (is_placeholder_ad)
	{
		ref_flags |= Ads_Advertisement_Candidate_Ref::FLAG_PLACEHOLDER_AD;
	}

	vslot->take_advertisement(ctx, candidate);

	Ads_Advertisement_Candidate_Ref *ref = new Ads_Advertisement_Candidate_Ref(candidate, vslot);
	ref->replica_id_ = candidate->references_.size();
	ref->flags_ |= ref_flags;
	ref->creative_ = creative;
	ref->variable_ad_duration_ = ad_duration;

	candidate->used_slots_.insert(vslot);
	candidate->references_.push_back(ref);
	ctx->selected_candidates_.insert(candidate);
	this->take_advertisement_candidate_reference(ctx, ref, ctx->delivered_candidates_);

	vslot->duration_remain_ = 0;
	if (vslot->parent_ != nullptr)
	{
		vslot->parent_->duration_remain_ = 0;
	}

	ADS_DEBUG((LP_DEBUG, "filled ad %s creative %s for slot[%d] %s with duration %lu\n",
		ADS_ENTITY_ID_CSTR(candidate->ad_->id_), ADS_ENTITY_ID_CSTR(creative->id_)
		, vslot->position_, vslot->custom_id_.c_str(), ad_duration));

	return 0;
}

// If slot enabled padding ad, CRO variable ad will be filled as padding ad with duration -1, no matter slot has unfilled duration or not.
// Else variable ad can be delivered when slot unfilled duration > 0.
// If slot has LAST_IN_BREAK scheduled ad, ADS ignores slot's padding ad setting and fills variable ad. If failed, fill placeholder ad.
int Ads_Selector::fill_variable_ad_into_slot(Ads_Selection_Context *ctx, Ads_Slot_Base *slot, Ads_Advertisement_Candidate *placeholder_candidate,
			Ads_GUID placeholder_creative_id, bool &variable_ad_filled)
{
	variable_ad_filled = false;
	Ads_Video_Slot *vslot = dynamic_cast<Ads_Video_Slot *>(slot);
	if (ctx == nullptr || slot == nullptr || vslot == nullptr)
	{
		ADS_LOG((LP_ERROR, "NULL slot met[%p/%p/%p]\n", ctx, slot, vslot));
		return -1;
	}

	if (vslot->parent_ != nullptr && !vslot->parent_->children_.empty() && slot != vslot->parent_->children_.back())
	{
		ADS_DEBUG((LP_DEBUG, "slot %s is not last sub slot\n", vslot->custom_id_.c_str()));
		return 0;
	}
	if (vslot->max_num_advertisements_ == 0)
	{
		ADS_DEBUG((LP_DEBUG, "slot %s max_num_advertisements_ is 0\n", vslot->custom_id_.c_str()));
		return 0;
	}
	if (ads::standard_ad_unit(vslot->time_position_class_.c_str()) == ads::OVERLAY)
	{
		ADS_DEBUG((LP_DEBUG, "slot %s is overlay\n", vslot->custom_id_.c_str()));
		return 0;
	}

	// calculate slot unfilled duration for variable duration ad.
	size_t slot_unfilled_duration = 0;
	if (vslot->min_duration() > vslot->duration_used())
	{
		slot_unfilled_duration = vslot->min_duration() - vslot->duration_used();
	}

	// Padding Ad should be the last ad in a slot. Refer FDB-5816 for more info
	// So, if last in slot ad is filled, we cannot fill the padding ad.
	bool is_filling_padding_ad = ctx->padding_ad_min_duration() > 0 && !vslot->has_last_in_slot_ad_filled_;

	size_t variable_ad_duration = -1;
	if (is_filling_padding_ad)
	{
		// fill as Padding Ad, the duration of padding ad is SUPPOSED to be -1, and will be adjusted by AdManager.
		// if (vslot->duration_remain_ < ctx->padding_ad_min_duration()) break;
	}
	else
	{
		// variable ad or placeholder can't filled if slot_unfilled_duration is 0
		if (slot_unfilled_duration == 0)
		{
			ADS_DEBUG((LP_DEBUG, "no duration remained in current slot %s\n", vslot->custom_id_.c_str()));
			return 0;
		}
		variable_ad_duration = slot_unfilled_duration;
		ADS_DEBUG((LP_DEBUG, "slot:%s, variable ad duration:%lu, parent slot min duration:%lu, parent slot duration used:%lu\n",
			vslot->custom_id_.c_str(), variable_ad_duration, vslot->min_duration(), vslot->duration_used()));
	}

	Ads_Advertisement_Candidate *variable_candidate = nullptr;
	const Ads_Creative_Rendition *variable_rendition = nullptr;
	// find suitable padding ad or variable ad
	for (auto *candidate : ctx->all_final_candidates_)
	{
		if (candidate == nullptr || !candidate->valid_)
		{
			ADS_DEBUG((LP_DEBUG, "met a null or invalid candidate\n"));
			continue;
		}
		if (is_filling_padding_ad)
		{
			/// padding ad must belong to CRO
			if (candidate->ad_ == nullptr || !candidate->ad_->is_house_ad(ctx->root_network_id(ads::MEDIA_VIDEO)))
			{
				ADS_DEBUG((LP_DEBUG, "ad[%p] is null or ad is not house ad\n", candidate->ad_));
				continue;
			}
		}

		const Creative_Rendition_List& creative_renditions = candidate->applicable_creatives(slot);
		for (const auto *rendition : creative_renditions)
		{
			if (rendition == nullptr)
			{
				ADS_LOG((LP_ERROR, "creative rendition is NULL\n"));
				continue;
			}
			if (rendition->duration_type_ == Ads_Creative_Rendition::VARIABLE)
			{
				variable_candidate = candidate;
				variable_rendition = rendition;
				break;
			}
		} // end creative loop

		if (variable_candidate != nullptr)
		{
			break;
		}
	}// end candidate loop

	if (vslot->schedule_mode_ != Ads_Slot_Template::Slot_Info::STATIC_SCHEDULE
		&& variable_candidate != nullptr && variable_rendition != nullptr)
	{
		// fill padding ad or variable ad, they are not allowed to be filled into static schedule slot
		if (fill_variable_ad(ctx, variable_candidate, variable_ad_duration, variable_rendition, vslot, false) >= 0)
		{
			ADS_DEBUG((LP_DEBUG, "success to fill variable ad, is_padding:%d\n", is_filling_padding_ad));
			variable_ad_filled = true;
			return 0;
		}
		else
		{
			ADS_LOG((LP_ERROR, "fail to fill variable ad, is_padding:%d\n", is_filling_padding_ad));
			return -1;
		}
	}
	else if (vslot->has_last_in_slot_ad_filled_ && placeholder_candidate != nullptr)
	{
		// placeholder can be delivered in the static schedule slot
		ADS_DEBUG((LP_DEBUG, "filling placeholder\n"));
		placeholder_candidate->duration_ = slot_unfilled_duration;
		const Ads_Creative_Rendition *placeholder_creative = nullptr;
		Ads_Advertisement_Candidate_Lite *placeholder_ad_lite = placeholder_candidate->lite_;
		bool dummy_is_phantom = false;
		if (placeholder_ad_lite == nullptr)
		{
			ADS_LOG((LP_ERROR, "placeholder_ad_lite is null\n"));
			return -1;
		}
		if (ctx->scheduler_->choose_nonauto_select_creative_for_digital_live(*ctx, *placeholder_ad_lite, *placeholder_candidate,
			slot, placeholder_creative, placeholder_creative_id, 0, false/*is_phantom_enabled*/, dummy_is_phantom) < 0 || placeholder_creative == nullptr)
		{
			ADS_LOG((LP_ERROR, "digital placeholder no applicable creative rendition found\n"));
			return -1;
		}
		if (placeholder_creative->duration_type_ != Ads_Creative_Rendition::VARIABLE)
		{
			ADS_LOG((LP_ERROR, "digital placeholder creative duration type is not VARIABLE\n"));
			return -1;
		}
		if (fill_variable_ad(ctx, placeholder_candidate, slot_unfilled_duration, placeholder_creative, vslot, true) >= 0)
		{
			ADS_DEBUG((LP_DEBUG, "success to fill placeholder variable add\n"));
			variable_ad_filled = true;
			// placeholder ad should not be regard as an ad
			--vslot->num_advertisements_;
			if (vslot->parent_)
			{
				--vslot->parent_->num_advertisements_;
			}
		}
		else
		{
			ADS_LOG((LP_ERROR, "fail to fill placeholder ad\n"));
			return -1;
		}
	}
	else
	{
		ADS_DEBUG((LP_DEBUG, "can not find variable ad for slot[%d] %s with unfilled duration %lu\n", vslot->position_, vslot->custom_id_.c_str(), slot_unfilled_duration));
	}

	return 0;
}

bool
Ads_Selector::test_creative_for_request_restriction(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate *candidate)
{
	if (ctx->request_->rep()->flags_ & Ads_Request::Smart_Rep::BYPASS_RESTRICTION)
		return true;

	Creative_Rendition_List tmp_creatives;
	selection::Error_Code error_code = selection::Error_Code::UNDEFINED;
	std::vector<selection::Error_Code> error_codes;
	for (Creative_Rendition_List::const_iterator it = candidate->creatives_.begin(); it != candidate->creatives_.end(); ++it)
	{
		const Ads_Creative_Rendition *creative = *it;
		if (!this->is_creative_applicable_for_request_restrictions(ctx, candidate, creative, error_code))
		{
			if (error_code > selection::Error_Code::UNDEFINED)
				error_codes.push_back(error_code);
			continue;
		}
		//OPP-2963 filter unsupported rendition by triggering concrete event
		if (!this->is_creative_applicable_for_triggering_concrete_event(ctx, candidate, creative))
			continue;

		if (!this->is_creative_applicable_for_rbp_proxy_content_type(*ctx, *candidate, *creative))
			continue;

		tmp_creatives.push_back(creative);
	}
	if (tmp_creatives.empty() && ctx->explicit_renditions_.find(candidate->ad()->id_) == ctx->explicit_renditions_.end() && !candidate->ad()->is_tracking())
	{
		ADS_DEBUG((LP_DEBUG, "candidate ad %s fail for no valid creative rendition\n", CANDIDATE_LITE_ID_CSTR(candidate->lite_)));

		candidate->lite_->set_error(selection::Error_Code::CREATIVE_RESTRICTION_CHECK_FAILED);
		for (auto error_code : error_codes)
			candidate->lite_->log_selection_error(error_code, selection::Error_Code::CREATIVE_RESTRICTION_CHECK_FAILED);

		return false;
	}

	candidate->creatives_.swap(tmp_creatives);
	return true;
}

bool
Ads_Selector::profile_matching(Ads_Selection_Context *ctx, Ads_Advertisement_Candidate* candidate, const Ads_Slot_Base* slot, std::vector<selection::Error_Code>* err_codes)
{
	const Ads_Advertisement* ad = candidate->ad();

	Creative_Rendition_List applicable_renditions;
	std::vector<selection::Error_Code> dummy_err_codes;
	if (err_codes == nullptr)
	{
		err_codes = &dummy_err_codes;
	}
	(*err_codes).clear();

	std::map<Ads_GUID, bool> bypass_filters;
	auto profile_unmatch_handler = [&](const Ads_Creative_Rendition *creative_rendition, const Ads_Slot_Base *slot, bool &bypass) {
		bypass = this->bypass_profile_matching(ctx, candidate, creative_rendition, slot, bypass_filters);

		if (!bypass && creative_rendition->creative_ != nullptr &&
				(candidate->is_pre_selection_external_translated() || creative_rendition->creative_->is_transcodable_nonwrapper_creative_))
		{
			candidate->lite_->jit_candidate_creatives_[slot].emplace(creative_rendition->creative_);
		}
	};

	Creative_Rendition_Set applicable_creatives;
	for (const Ads_Creative_Rendition *creative : candidate->creatives_)
	{
		selection::Error_Code error = selection::Error_Code::UNDEFINED;
		if (!this->is_creative_applicable_for_slot(ctx, candidate, creative, slot, false, &error, profile_unmatch_handler))
		{
			(*err_codes).push_back(error);
			continue;
		}
		/// checking secondary ad units
		if (ad->ad_unit()
				&& slot->env() == ads::ENV_VIDEO
				&& slot->ad_unit_ && slot->match_ad_unit()
				&& slot->ad_unit_->network_id_ == ad->network_id_  		// O&O
				&& slot->ad_unit_->id_ != ad->ad_unit()->effective_id()) 	// and a secondary ad unit
		{
			const Ads_Video_Slot *vslot = reinterpret_cast<const Ads_Video_Slot *>(slot);
			std::map<Ads_GUID, Ads_Video_Slot::Ad_Unit_Restriction>::const_iterator it = vslot->ad_unit_restrictions_.find(ad->ad_unit()->effective_id());
			if (it != vslot->ad_unit_restrictions_.end())
			{
				const Ads_Slot_Restriction& slot_restriction = it->second;
				if (!this->is_creative_applicable_for_slot(ctx, candidate, creative, slot, &slot_restriction, &error, profile_unmatch_handler))
				{
					(*err_codes).push_back(error);
					continue;
				}
			}
		}

		// check split avail ad creative rendition
		if (ctx->scheduler_->is_linear_live_schedule())
		{
			const Ads_Video_Slot *vslot = slot->env() == ads::ENV_VIDEO ? dynamic_cast<const Ads_Video_Slot *>(slot) : nullptr;
			if (vslot == nullptr 
				||!ctx->scheduler_->is_creative_rendition_check_passed(*ctx, *creative, *vslot, *candidate, Ads_Scheduler::CREATIVE_SELECT_TYPE::SPLIT_AVAIL_ADDRESSABLE))
			{
				ADS_DEBUG((LP_DEBUG, "slot %s is not applicable for candidate %s\n", slot->custom_id_.c_str(), ADS_ENTITY_ID_CSTR(candidate->ad_id())));
				continue;
			}
		}

		if (creative->rendition_ext_info_ != nullptr)
		{
			creative->rendition_ext_info_->profile_check_passed_ = true;
		}

		applicable_renditions.push_back(creative);
		if (creative->creative_ != nullptr)
			applicable_creatives.insert(creative->creative_);
	}

	if (applicable_renditions.empty())
	{
		candidate->non_applicable_slots_.insert(slot);
		return false;
	}

	if (candidate->is_scheduled_in_slot(slot))
	{
		candidate->applicable_scheduled_creatives(slot).swap(applicable_renditions);
	}
	else
	{
		candidate->applicable_creatives(slot).swap(applicable_renditions);
		candidate->lite_->filter_jit_candidate_creatives(slot, applicable_creatives);
	}

	return true;
}

bool
Ads_Selector::bypass_profile_matching(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate *candidate,
					const Ads_Creative_Rendition *creative, const Ads_Slot_Base *slot,
					std::map<Ads_GUID, bool> &bypass_filters)
{
	if (!ctx->is_external_creative_translation_applicable(creative) && !candidate->lite_->is_pg_ad())
		return false;

	if (!candidate->lite_->jitt_enablement(Ads_Advertisement_Candidate_Lite::JiTT_Config::VAST_NON_MARKET).is_enabled())
		return false;

	if (candidate->ad_->is_external_ || ctx->bypass_profile_matching_for_jitt_)
	{
		ADS_DEBUG((LP_DEBUG, "Ad %s creative %s rendition %s succeeded to bypass profile matching, is exteranl: %s, enforce bypass: %s\n",
					ADS_ENTITY_ID_CSTR(candidate->ad_->id_),
					ADS_ENTITY_ID_CSTR(creative->creative_id_),
					CREATIVE_EXTID_CSTR(creative),
					candidate->ad_->is_external_ ? "true" : "false",
					ctx->bypass_profile_matching_for_jitt_ ? "true" : "false"));
		return true;
	}

	if (creative->primary_asset_ != nullptr)
	{
		if (!selection::JiTT::is_transcodable_content_type(creative->primary_asset_->mime_type_.c_str()))
		{
			ADS_DEBUG((LP_DEBUG, "Ad %s creative %s rendition %s bypass faliure for rendition not transcodable, content type %s\n"
				, ADS_ENTITY_ID_CSTR(candidate->ad_->id_), ADS_ENTITY_ID_CSTR(creative->creative_id_), CREATIVE_EXTID_CSTR(creative)
				, creative->primary_asset_->mime_type_.c_str()));
			return false;
		}
	}
	else
	{
		ADS_DEBUG((LP_DEBUG, "Ad %s creative %s rendition %s bypass faliure for no primary asset\n",
			ADS_ENTITY_ID_CSTR(candidate->ad_->id_), ADS_ENTITY_ID_CSTR(creative->creative_id_), CREATIVE_EXTID_CSTR(creative)));
		return false;
	}

	// ESC-21258
	auto filter_it = bypass_filters.find(creative->creative_id_);
	if (filter_it != bypass_filters.end())
	{
		ADS_DEBUG((LP_DEBUG, "Ad %s creative %s already tried to bypass profile matching, bypassed: %s. Skip\n",
					ADS_ENTITY_ID_CSTR(candidate->ad_->id_),
					ADS_ENTITY_ID_CSTR(creative->creative_id_),
					filter_it->second ? "true" : "false"));

		return filter_it->second;
	}

	bool bypass = check_jitt_status_for_bypass_profile_matching(ctx, candidate, creative, slot);
	bypass_filters.emplace(creative->creative_id_, bypass);
	return bypass;
}

bool
Ads_Selector::check_jitt_status_for_bypass_profile_matching(Ads_Selection_Context *ctx, const Ads_Advertisement_Candidate *candidate,
							const Ads_Creative_Rendition *creative_rendition, const Ads_Slot_Base *slot)
{
	auto cch_creatives_range = ctx->repository_->mrm_creative2cch_creatives_.equal_range(creative_rendition->creative_id_);
	if (cch_creatives_range.first == cch_creatives_range.second)
	{
		ADS_DEBUG((LP_DEBUG, "Ad %s creative %s rendition %s succeeded to bypass profile matching. No CCH creative was found for mrm creative\n",
				ADS_ENTITY_ID_CSTR(candidate->ad_->id_), ADS_ENTITY_ID_CSTR(creative_rendition->creative_id_), CREATIVE_EXTID_CSTR(creative_rendition)));
		return true;
	}

	for (auto it = cch_creatives_range.first; it != cch_creatives_range.second; ++it)
	{
		Ads_String ad_id = it->second.c_str();
		selection::JiTT::Accomplishment jit_ac;
		ctx->jitt().get_jit_accomplishment(ad_id, jit_ac);

		if (jit_ac.jit_creative_ != nullptr && jit_ac.jit_task_status_ == Ads_JiT_Creative::NO_TASK)
			continue;

		if (jit_ac.jit_creative_ == nullptr || jit_ac.jit_renditions_.empty())
		{
			ADS_DEBUG((LP_DEBUG, "Ad %s creative %s rendition %s failed to bypass profile matching. CCH creative %s has no jit rendition\n",
						ADS_ENTITY_ID_CSTR(candidate->ad_->id_), ADS_ENTITY_ID_CSTR(creative_rendition->creative_id_),
						CREATIVE_EXTID_CSTR(creative_rendition), ad_id.c_str()));
			return false;
		}

		bool found = false;
		for (auto *r : jit_ac.jit_renditions_)
		{
			Ads_Creative_Rendition faked_rendition;
			faked_rendition.inherit_from(*creative_rendition, *creative_rendition->creative_);
			if (!r->fake_creative_rendition(*ctx->repository_, faked_rendition, true))
				continue;

			faked_rendition.id_ = r->id_;
			faked_rendition.creative_id_ = jit_ac.jit_creative_->creative_id_;

			for (const auto p : slot->profiles_)
			{
				if (Ads_Selector::is_creative_applicable_for_profile(ctx, slot->env(), candidate, &faked_rendition, p, slot, 0))
				{
					found = true;
					break;
				}
			}
			if (found)
				break;
		}

		if (!found)
		{
			ADS_DEBUG((LP_DEBUG, "Ad %s creative %s rendition %s failed to bypass profile matching. No jit rendition of CCH creative %s is applicable for any profile\n",
					ADS_ENTITY_ID_CSTR(candidate->ad_->id_), ADS_ENTITY_ID_CSTR(creative_rendition->creative_id_), CREATIVE_EXTID_CSTR(creative_rendition), ad_id.c_str()));
			return false;
		}
	}

	ADS_DEBUG((LP_DEBUG, "Ad %s creative %s rendition %s succeeded to bypass profile matching. All CCH creatives pass profile matching\n",
			ADS_ENTITY_ID_CSTR(candidate->ad_->id_), ADS_ENTITY_ID_CSTR(creative_rendition->creative_id_), CREATIVE_EXTID_CSTR(creative_rendition)));
	return true;
}

void Ads_Selector::try_fill_ad_pod(Ads_Selection_Context* ctx, Ads_Advertisement_Candidate* candidate, Ads_Slot_Base* slot, Ads_Advertisement_Candidate_Set& filters, int64_t& profit)
{
	if (!candidate->lite_->pod_ad_info()) return;
	Ads_Advertisement_Candidate_List candidates;
	candidate->collect_applicable_pod_ads(ctx, slot, filters, candidates);

	for (Ads_Advertisement_Candidate* pod_ad : candidates)
	{
		pod_ad->replacable(slot, false);
		if (pod_ad->is_leader_pod_ad())
		{
			ADS_DEBUG((LP_DEBUG, "pod ad %s with ext_id %d has already taken slot %s\n", ADS_ENTITY_ID_CSTR(pod_ad->ad_->id_), pod_ad->pod_replica_id(), slot->custom_id_.c_str()));
			continue;
		}
		ADS_ASSERT(pod_ad->is_follower_pod_ad());

		ADS_DEBUG((LP_DEBUG, "pod ad %s with ext_id %d is gonna take slot %s\n", ADS_ENTITY_ID_CSTR(pod_ad->ad_->id_), pod_ad->pod_replica_id(), slot->custom_id_.c_str()));
		pod_ad->slot_ = slot;
		static_cast<Ads_Video_Slot*>(slot)->take_advertisement(ctx, pod_ad);
		filters.insert(pod_ad);
		profit += pod_ad->effective_eCPM();
	}
}

size_t
Ads_Selector::calculate_inventory_protection_flags(const Ads_Repository *repo, const Ads_Advertisement_Candidate_Ref *candidate_ref) const
{
	if ( !candidate_ref || !candidate_ref->candidate_ || !candidate_ref->candidate_->lite_ )
		return 0;

	size_t flags = 0;
	if (candidate_ref->external_translated_)
	{
		flags |= report::Request_Log_Record_Advertisement::POST_SELECTION_EXTERNAL_TRANSLATED;
	}
	if (candidate_ref->candidate_->is_pre_selection_external_translated() && candidate_ref->slot_->env() == ads::ENV_VIDEO)
	{
		flags |= report::Request_Log_Record_Advertisement::PRE_SELECTION_EXTERNAL_TRANSLATED;
	}
	if (candidate_ref->candidate_->lite_->is_openrtb())
	{
		flags |= report::Request_Log_Record_Advertisement::OPENRTB;
	}

	if (candidate_ref->external_ad_info_.has_jit_renditions_)
	{
		flags |= report::Request_Log_Record_Advertisement::HAS_JIT_RENDITIONS;
		if (candidate_ref->external_ad_info_.has_adstor_renditions_)
			flags |= report::Request_Log_Record_Advertisement::HAS_ADSTOR_RENDITIONS;
	}
	else if (candidate_ref->creative_ && candidate_ref->creative_->transcode_trait_)
	{
		if (!candidate_ref->creative_->ad_asset_store_info_.empty())
		{
			flags |= report::Request_Log_Record_Advertisement::HAS_ADSTOR_RENDITIONS;
		}
		if (candidate_ref->creative_->transcode_trait_->source_ == Ads_Creative_Rendition::Transcode_Trait::PRE_TRANSCODING)
		{
			flags |= report::Request_Log_Record_Advertisement::PRE_TRANSCODED;
		}
		else
		{
			flags |= report::Request_Log_Record_Advertisement::HAS_JIT_RENDITIONS;
		}
	}

	if (candidate_ref->external_ad_info_.has_client_renditions_)
	{
		flags |= report::Request_Log_Record_Advertisement::HAS_CLIENT_RENDITIONS;
	}
	else if (candidate_ref->creative_)
	{
		auto *external_creative_info = candidate_ref->candidate_->lite_->external_creative_info(candidate_ref->creative_->creative_);
		if (external_creative_info && !external_creative_info->client_renditions_.empty())
			flags |= report::Request_Log_Record_Advertisement::HAS_CLIENT_RENDITIONS;
	}
	return flags;
}

void
Ads_Selector::get_player_concrete_events(const Ads_Repository& repo, const Ads_Environment_Profile& profile, Ads_Player_Concrete_Events_Map& player_concrete_events)
{
	for(const auto& item : profile.renderer_parameters_)
	{
		const Ads_String& name = item.first.c_str();
		const Ads_String& value = item.second.c_str();
		if(name.compare("fwImpressionMap") != 0)
			continue;
		get_player_concrete_events(repo, value, player_concrete_events);
	}
}

void
Ads_Selector::get_player_concrete_events(const Ads_Repository& repo, const Ads_String& value, Ads_Player_Concrete_Events_Map& player_concrete_events)
{
	Ads_String_List value_items;
	if(ads::split(value, value_items, ';') > 0)
	{
		for(const auto& value_item : value_items)
		{
			Ads_String entry = ads::trim(value_item);
			if(entry.empty())
				continue;
			Ads_String_List entry_items;
			if(ads::split(entry, entry_items, ':') == 3)
			{
				Ads_String event_type = ads::trim(entry_items[0]);
				Ads_String event_name = ads::trim(entry_items[1]);
				Ads_String event_ids = ads::trim(entry_items[2]);
				if (event_type.empty() || event_name.empty() || event_ids.empty())
					continue;
				Ads_String_List event_items;
				if (ads::split(event_ids, event_items, ',') > 0)
				{
					for(const auto& event_id : event_items)
					{
						Ads_String event_item = ads::trim(event_id);
						if (event_item.empty())
							continue;
						Ads_GUID concrete_event_id = ads::str_to_entity_id(ADS_ENTITY_TYPE_CONCRETE_EVENT, event_item);
						auto cit_concrete_event = repo.concrete_events_.find(concrete_event_id);
						if (cit_concrete_event == repo.concrete_events_.end())
							continue;
						player_concrete_events[event_type][event_name].insert(concrete_event_id);
					}
				}
			}
		}
	}
}

unsigned short
Ads_External_Ad_Info::buyer_priority() const
{
	auto buyer_priority_override = bid_info().deal_ ? bid_info().deal_->buyer_priority_override_ :
		bid_info().buyer_group_ ? bid_info().buyer_group_->buyer_priority_override_ : 0;

	if (buyer_priority_override != 0)
		return buyer_priority_override;

	bool honor_bidding_buyer = false;
	if (bid_info().rtb_auction_ && bid_info().rtb_auction_->network().config() != nullptr)
		honor_bidding_buyer = bid_info().rtb_auction_->network().config()->honor_bidding_buyer_;

	return honor_bidding_buyer ? (bid_info().bidding_buyer_ ? bid_info().bidding_buyer_->priority_ : Ads_Buyer::DEFAULT_PRIORITY) :
		(bid_info().buyer_ ? bid_info().buyer_->priority_ : Ads_Buyer::DEFAULT_PRIORITY);
}

const Ads_Buyer_Platform*
Ads_External_Ad_Info::Bid_Info::buyer_platform() const
{
	return rtb_auction_ ? rtb_auction_->buyer_platform() : nullptr;
}

Ads_GUID
Ads_External_Ad_Info::Bid_Info::auction_network_id() const
{
	return rtb_auction_ ? rtb_auction_->network().id_ :
		(pg_auction_ctx_? pg_auction_ctx_->auction_owner_.network_.id_ : ADS_INVALID_NETWORK_ID);
}

Ads_String
Ads_External_Ad_Info::Bid_Info::encode_price_with_doubleclick_algo(double price) const
{
	const auto* bp = buyer_platform();
	if (bp == nullptr || bp->price_shared_encryption_key_.empty() || bp->price_shared_integrity_key_.empty())
		return "";
	// Encoding result will be random every time if no init_vec
	const size_t INITV_SIZE = 16;
	const Ads_String init_vec(INITV_SIZE, '\0');
	DoubleClick_Price_Crypto doubleclick_price_crypto(ads::base64_decode(bp->price_shared_encryption_key_.c_str(), true),
		ads::base64_decode(bp->price_shared_integrity_key_.c_str(), true));
	return doubleclick_price_crypto.encode_price_value(price, &init_vec);;
}

void
Ads_External_Ad_Info::Bid_Info::set_bidding_buyer(const Ads_Repository *repo)
{
	if (buyer_ != nullptr && buyer_->seat_ != nullptr && buyer_->seat_->external_seat_id_ == DEFAULT_SEAT_ID)
	{
		is_any_buyer_transaction_ = true;

		const Ads_Seat *seat = nullptr;
		if ((buyer_platform() != nullptr && buyer_platform()->find_seat(seat_id_, seat) >= 0 && seat != nullptr)
			&& (rtb_auction_->network().find_buyer(seat->network_id_, bidding_buyer_) < 0 || bidding_buyer_ == nullptr))
		{
			is_from_unknown_buyer_ = true;
		}
	}
	if (bidding_buyer_ == nullptr)
		bidding_buyer_ = buyer_;
}

Ads_String
Ads_External_Ad_Info::Bid_Info::bid_request_id() const
{
	return rtb_auction_ ? rtb_auction_->bid_request_id_ : "";
}

void
Ads_Selector::parse_tracking_events(const Ads_Selection_Context& ctx, const Ads_String& tracking_events, std::map<Ads_String, Ads_String>& tracking_events_map)
{
	Ads_String err;
	json11::Value tracking_events_value = json11::parse(tracking_events, err);
	if (!err.empty())
	{
		ADS_LOG((LP_ERROR, "failed to parse tracking events: %s\n", err.c_str()));
		return;
	}

	const json11::Object& tracking_events_object = tracking_events_value.object_value();
	for(auto& item : tracking_events_object)
	{
		const Ads_String& tracking_origin_url = item.second.string_value();
		Ads_String tracking_url;
		if(!tracking_origin_url.empty())
		{
			Ads_Macro_Expander::instance()->translate(ctx.menv_, tracking_origin_url, tracking_url, false);
		}
		tracking_events_map[item.first] = tracking_url;
	}
}

int
Ads_Selector::finalize_geo_info(const Ads_Repository &repo, const Ads_GUID distributor_network_id, bool is_linear_live, Ads_Geo_Info &geo_info, Ads_Audience &audience)
{
	if (audience.sis_info_.is_zipcode_valid())
	{
		geo_info.set_geo_info_by_audience_data(repo, "", "", audience.sis_info_.zipcode_, Ads_Geo_Info::SOURCE_PRIORITY_SIS);
		if (geo_info.is_sis_geo_used())
			audience.set_flag(AUDIENCE_FLAG::FLAG_USE_SIS_GEO);
	}
	if (audience.is_geo_as_audience_enabled())
	{
		// geo as audience
		audience.extract_geo_info_from_segments(repo, distributor_network_id);
		if (audience.geo_info_.is_geo_info_completed())
		{
			geo_info.set_geo_info_by_audience_data(repo, audience.geo_info_.country_, audience.geo_info_.postalcode_, audience.geo_info_.zipcode_, Ads_Geo_Info::SOURCE_PRIORITY_AUDIENCE);
			if (geo_info.is_geo_as_audience_used())
				audience.set_flag(AUDIENCE_FLAG::FLAG_USE_GEO_AS_AUDIENCE);
		}
		else if (audience.geo_info_.is_geo_segment_partial())
		{
			audience.set_flag(AUDIENCE_FLAG::FLAG_PARTIAL_GEO_SEGMENT);
		}
	}
	if (geo_info.need_postal_code_infer())
		geo_info.retrieve_geo_info_by_postalcode(distributor_network_id, repo);
	geo_info.check_geo_info_status();
	if (geo_info.need_ip_inference())
		geo_info.complement_geo_info_by_ip(geo_info.remote_addr());
	if (is_linear_live)
		geo_info.override_geo_info_based_on_syscode(repo.syscode_zone_dma_map_);
	ADS_DEBUG((LP_TRACE,
		"geo: received a request from %s: %s,%s,%s,%d,%s\n",
		geo_info.remote_addr().c_str(),
		geo_info.country_.c_str(),
		geo_info.state_.c_str(),
		geo_info.city_.c_str(),
		geo_info.dma_code_,
		geo_info.postal_code_.c_str()));
	return 0;
}

void
Ads_Selector::generate_candidate_restriction_log(const Ads_Selection_Context *ctx, json11::Object &restriction_log) const
{
	if (ctx == nullptr)
		return;

	restriction_log["transaction_id"] = ctx->transaction_id_;
	restriction_log["video_cro_network_id"] = ctx->root_asset_ != nullptr ? ads::entity::str(ctx->root_asset_->network_id_) : "-1";
	restriction_log["root_asset_id"] = ctx->root_asset_ != nullptr ? ads::entity::str(ctx->root_asset_->id_) : "-1";
	restriction_log["root_site_section_id"] = ctx->root_section_ != nullptr ? ads::entity::str(ctx->root_section_->id_) : "-1";
	restriction_log["profile_id"] = ctx->profile_ != nullptr ? ads::entity::str(ctx->profile_->id_) : "-1";
	restriction_log["fourfronts_network"] = ctx->candidate_restriction_tracking_network_id_;

	json11::Array restricted_ads;
	for (auto& log : ctx->restricted_candidates_logs_)
	{
		const Ads_Advertisement *ad = nullptr;
		if (ctx->find_advertisement(log.first, ad) < 0 || ad == nullptr)
			continue;

		json11::Object restricted_ad;
		restricted_ad["ad_id"] = ads::entity::str(ads::entity::restore_looped_id(log.first));
		restricted_ad["ad_oo_network_id"] = ads::entity::str(ad->network_id_);
		restricted_ad["restriction_reason"] = log.second.reason_;

		json11::Array sub_reasons;
		for (const auto &sub_reason : log.second.sub_reasons_)
		{
			sub_reasons.push_back(sub_reason);
		}
		restricted_ad["sub_reasons"] = sub_reasons;

		restricted_ads.push_back(restricted_ad);
	}
	restriction_log["restricted_ads"] = restricted_ads;
	Ads_Server::elk_log("candidate_restriction_tracking", restriction_log);

	return;
}

void
Ads_Selector::output_ad_candidate_error_log(const Ads_Selection_Context *ctx) const
{
	// output selection troubleshooting log
	if (!(Ads_Server_Config::instance()->enable_troubleshooting_logging_ &&
		(::rand() % 100 < ctx->repository_->system_config().yield_optimization_volume_cap_troubleshooting_logging_ratio_)))
	{
		return;
	}

	selection::Error_Collection::Attributed_Errors errors;
	ctx->error_collection_.get_errors_with_search_index({selection::Error_Collection::ATTRIBUTION_TYPE::ADVERTISEMENT}, errors);

	json11::Array ad_errors;
	for (const auto* err : errors)
	{
		json11::Object tags;
		// currently, hardcode for yield optimization volume cap troubleshooting, for the future, when meets more similar requirements we need refactor here.
		if (err->category_ == selection::Error_Code ::MET_YIELD_OPT_VOLUME_CAP)
		{
			tags["yield_optimization_cap"] = 1;
		}
		else
			continue;

		json11::Object ad_error;
		err->to_json(ad_error);
	
		if (!tags.empty())
			ad_error["tags"] = std::move(tags);

		ad_errors.push_back(std::move(ad_error));
	}

	if (ad_errors.empty())
	{
		return;
	}

	json11::Object log;
	log["transaction_id"] = ctx->transaction_id_;
	log["ad_errors"] = std::move(ad_errors);
	Ads_Server::elk_log("ads_selection_ad_errors", log);
}

void
Ads_Selector::serialize_and_store_binary_log_record(const Ads_Request *request, report::Request_Log_Record &record, bool is_for_unclearing_ad)
{
	LOG_VERSION(record);

	Ads_Selection_Context::APM::calculate_duration(request, record);

	record.set_kafka_msg_key(ads::kafka_key(record.transaction_id(), record.server_id()));
	serialize_and_store_binary_log_record_i(record, record.kafka_msg_key(), record.server_id(), record.transaction_id(), true, is_for_unclearing_ad);
}

void
Ads_Selector::serialize_and_store_binary_log_record(report::Callback_Log_Record &record, bool is_for_unclearing_ad)
{
	record.set_kafka_msg_key(ads::kafka_key(record.transaction_id(), record.server_id()));
	serialize_and_store_binary_log_record_i(record, record.kafka_msg_key(), record.server_id(), record.transaction_id(), false, is_for_unclearing_ad);
}

void
Ads_Selector::serialize_and_store_binary_log_record(fastlane::Request_Record &record, const Ads_String &kafka_key)
{
	serialize_and_store_binary_log_record_i(record, kafka_key.empty()? ads::kafka_key(record.transaction_id(), record.server_id()) : kafka_key, record.server_id(), record.transaction_id(), true, false, true);
}

void
Ads_Selector::serialize_and_store_binary_log_record(fastlane::Callback_Record &record, const Ads_String &kafka_key)
{
	serialize_and_store_binary_log_record_i(record, kafka_key.empty()? ads::kafka_key(record.transaction_id(), record.request_server_id()) : kafka_key, record.request_server_id(), record.transaction_id(), false, false, true);
}

void
Ads_Selector::serialize_and_store_binary_log_record_i(const ::google::protobuf::Message &message, const Ads_String &kafka_key, const Ads_String &server_id, 
		const Ads_String &transaction_id, bool is_request, bool is_for_unclearing_ad, bool is_fastlane_binlog)
{
	bool kafka = is_fastlane_binlog ? Ads_Log_Service::allow_fastlane_log_output_kafka() : Ads_Log_Service::allow_log_output_kafka();
	bool external = (server_id != Ads_Server_Config::instance()->id_);
	Ads_Binary_Log_Record::TYPE type = is_request ? Ads_Binary_Log_Record::TYPE_REQUEST : Ads_Binary_Log_Record::TYPE_ACK;
	// fill in binary log record
	auto lb = new Ads_Binary_Log_Record();
	lb->type_ = type;
	lb->application_ = is_for_unclearing_ad ? Ads_Binary_Log_Record::MARKETPLACE_UNCLEARING_LOG : (is_fastlane_binlog ? Ads_Binary_Log_Record::FASTLANE_LOG : Ads_Binary_Log_Record::NORMAL_LOG);
	lb->kafka_ = kafka;
	lb->kafka_key_ = kafka_key;
	lb->partition_key_ = transaction_id + server_id;
	if (Ads_Binary_Log_Record::TYPE_ACK == type) { lb->external_ack_ = external; }
	if (!message.SerializeToString(&lb->content_)) { lb->destroy(); }
	else {
		auto msg_type = Ads_Logger_Message::MESSAGE_BINARY_LOG_RECORD;
		auto msg = Ads_Message_Base::create(msg_type, (void *)lb);
		if (Ads_Log_Service::instance()->post_message(msg) < 0) {
			lb->destroy();
			msg->destroy();
		}
	}
	return;
}

int
Ads_Selector::reset_root_asset_to_ad_router_rovn(const Ads_Repository &repo, Ads_Selection_Context &ctx) const
{
	if (ctx.root_section_ == nullptr)
		return -1;

	if (ctx.root_asset_ != nullptr && ctx.root_asset_->network_id_ == ctx.root_section_->network_id_)
	{
		ADS_DEBUG((LP_DEBUG, "ad router: doesn't reset CRO as asset cro %s = section cro %s\n",
			ADS_ENTITY_ID_CSTR(ctx.root_asset_->network_id_),
			ADS_ENTITY_ID_CSTR(ctx.root_section_->network_id_)));
		return -1;
	}

	const Ads_Network *root_section_network = nullptr;
	repo.find_network(ctx.root_section_->network_id_, root_section_network);
	if (root_section_network == nullptr)
	{
		ADS_DEBUG((LP_DEBUG, "ad router: section cro network(aka ad router network) not found: %s\n",
			ADS_ENTITY_ID_CSTR(ctx.root_section_->network_id_)));
		return -1;
	}
	ctx.original_root_asset_id_ = ctx.root_asset_id_;
	ctx.original_root_asset_ = ctx.root_asset_;
	ctx.root_asset_id_ = root_section_network->run_of_network_id(ads::MEDIA_VIDEO);
	ctx.find_asset(ctx.root_asset_id_, ctx.root_asset_);
	ADS_DEBUG((LP_DEBUG, "ad router: reset root asset to section cro network(aka ad router network): %s, ROVN: %s \n",
		ADS_ENTITY_ID_CSTR(ctx.root_section_->network_id_),
		ADS_ENTITY_ID_CSTR(ctx.root_asset_id_)));

	return 0;
}

int
Ads_Selector::initialize_website_root(Ads_Selection_Context *ctx) const
{
	const Ads_Section_Base *website_root = this->website_root(reinterpret_cast<const Ads_Section_Base *>(ctx->section_), ctx->repository_, ctx);
	if (website_root == nullptr)
	{
		//if website root not set, return parent site
		website_root = reinterpret_cast<Ads_Section_Base *>(const_cast<Ads_Asset_Base *>(ctx->section_->site_p(ctx->repository_)));
		if (website_root != nullptr)
		{
			ADS_DEBUG((LP_DEBUG, "website root: no parent having session duration found for root section %s, fallback to parent site %s as candidate website root\n",
				ADS_ENTITY_ID_CSTR(ctx->section_->id_),
				ADS_ENTITY_ID_CSTR(website_root->id_)));
		}
		else
		{
			ADS_DEBUG((LP_DEBUG, "website root: no parent having session duration found for root section %s\n",
				ADS_ENTITY_ID_CSTR(ctx->section_->id_)));
		}
	}
	else
	{
		ADS_DEBUG((LP_DEBUG, "website root: candidate website root with session duration found %s, session duration %d\n",
			ADS_ENTITY_ID_CSTR(website_root->id_),
			website_root->session_duration_));
	}

	bool first_request_in_session = !website_root;

	if (website_root != nullptr)
	{
		if (ctx->user_->website_root_id() != website_root->id_)
		{
			const Ads_Asset_Base *previous_root = 0;

			///website_root stickiness
			if (ctx->find_section(ctx->user_->website_root_id(), previous_root) < 0
			    || !previous_root
			    || !ctx->is_parent_asset(ctx->section_->id_, ctx->user_->website_root_id(), 0)
			    || ctx->is_parent_asset(website_root->id_, ctx->user_->website_root_id(), 0)
			    || (website_root->session_duration_ > 0 && reinterpret_cast<const Ads_Section_Base *>(previous_root)->session_duration_ <= 0))
			{
				ADS_DEBUG((LP_DEBUG, "website root: website root changed, use new website root %s, previous one is %s\n",
					ADS_ENTITY_ID_CSTR(website_root->id_),
					ADS_ENTITY_ID_CSTR(ctx->user_->website_root_id())));
				ctx->user_->update_website_root_id(website_root->id_);
			}
			else
			{
				ADS_DEBUG((LP_DEBUG, "website root: website root not changed, still use previous website root %s\n",
					ADS_ENTITY_ID_CSTR(ctx->user_->website_root_id())));
			}
		}

		if (ctx->user_->has_section_context_record(website_root->id_) == false)
		{
			first_request_in_session = true;
			ADS_DEBUG((LP_DEBUG, "website root: no session context found for website root %s, should be first request in session\n",
				ADS_ENTITY_ID_CSTR(website_root->id_)));
		}
	}
	else if (ctx->section_network_ != nullptr)
	{
		Ads_GUID rosn_id = ctx->section_network_->run_of_network_id(ads::MEDIA_SITE_SECTION);
		ctx->user_->update_website_root_id(rosn_id);
		ADS_DEBUG((LP_DEBUG, "website root: no website root founded, will use ROSN %s as new website root\n",
			ADS_ENTITY_ID_CSTR(rosn_id)));
	}

	if (first_request_in_session && ctx->user_->has_section_context_record(ctx->user_->website_root_id()) == false)
	{
		size_t ttl = ((website_root != NULL) && (website_root->session_duration_ > 0)) ?website_root->session_duration_ : (size_t)Ads_Request::TTL_8_HOURS;
		ctx->user_->add_section_context_record(ctx->user_->website_root_id(), ctx->time(), ctx->time(), ttl, 0, 0);
		ctx->user_->set_first_request_in_session(true);
	}
	return 0;
}

int Ads_Selector::get_tracking_urls(const Ads_Repository *repo, Ads_Macro_Environment& menv,
		Ads_GUID ad_id, Ads_GUID creative_rendition_id, std::vector<Ads_String>& urls,
		const Ads_String& callback_name)
{
	ADS_DEBUG((LP_TRACE, "Collecting tracking urls\n"));
	const Ads_Advertisement *ad = 0;
	const Ads_Creative_Rendition *cr = 0;
	repo->find_advertisement(ad_id, ad);
	repo->find_creative_rendition(creative_rendition_id, cr);

	if (!ad || !cr)
	{
		ADS_LOG((LP_ERROR, "ad %s: %08x, creative rendition %s: %08x not found\n",
			ADS_ENTITY_ID_CSTR(ad_id), ad, ADS_ENTITY_ID_CSTR(creative_rendition_id), cr));
		return -1;
	}

	ADS_DEBUG((LP_TRACE, "ad %s, creative %s\n", ADS_ENTITY_ID_CSTR(ad_id), ADS_ENTITY_ID_CSTR(cr->creative_id_)));

	const Ads_Advertisement::Action_RVector *actions = ad->actions(Ads_Creative_Rendition::URL_IMPRESSION, cr);
	if(!actions || actions->empty())
		actions = ad->actions(Ads_Creative_Rendition::URL_IMPRESSION);

	if(!actions || actions->empty())
		return 0;

	menv.ad_ = ad;
	menv.creative_ = cr;

	//collect tracking url
	for(Ads_Advertisement::Action_RVector::const_iterator ait = actions->begin();ait != actions->end();++ait)
	{
		const Ads_Advertisement::Action *act = *ait;
		if(!act->name_.compare(callback_name.c_str()))
		{
			Ads_String url;
			Ads_Macro_Expander::instance()->translate(&menv, act->url_.c_str(), url, false);
			urls.push_back(url);
			ADS_DEBUG((LP_TRACE, "\t%s\n", url.c_str()));
		}
	}

	return urls.size();
}

// FIXME: move to Ads_Frequency_Cap
bool 
Ads_Selector::has_frequency_cap(const Ads_Advertisement &ad, const Ads_Selection_Context &ctx, bool check_companion) const
{
	if (!check_companion && ad.frequency_exempt())
		return false;
	
	const auto* io = ctx.get_ad_io(ad);
	return (ad.placement_ && !ad.placement_->frequency_caps_.empty())
		   || (io && !io->frequency_caps_.empty()) || (ad.campaign_ && !ad.campaign_->frequency_caps_.empty());
}

const Ads_User_Experience_Config * Ads_Selector::ux_conf(const Ads_Asset_Base* asset,
		const Ads_Repository *repo, const Ads_Forecast_Scenario* forecast_scenario,
		const Ads_Selection_Context *ctx) const
{
	const Ads_User_Experience_Config *ret = 0;
	int max = -1;

	Ads_GUID_Set processed;
	Ads_GUID_List pending;
	pending.push_back(asset->id_);

	if (ads::entity::type(asset->id_) == ADS_ENTITY_TYPE_ASSET && !ads::asset::is_group(asset->id_))
    {
        const Ads_Network* network = 0;
        if (repo->find_network(asset->network_id_, network) >= 0 && network)
            network->try_apply_group_rules(asset, pending);
    }

	while (!pending.empty())
	{
		Ads_GUID current_id = pending.front();
		pending.pop_front();

		if (processed.find(current_id) != processed.end()) continue;
		processed.insert(current_id);

		const Ads_Asset_Base *current = 0;
		if (current_id == asset->id_)
		{
			current = asset;
		}
		else if (ctx != nullptr && (ctx->find_asset_or_section(current_id, current) < 0 || !current))
		{
			continue;
		}
		else if (Ads_Repository::find_asset_or_section(repo, current_id, current) < 0 || !current)
		{
			continue;
		}

		int precedence = current->is_run_of_network() ? 0 :
			current->is_ros() && current != asset ? 1 :
			current->is_group() && !current->is_series_or_site() ? 2 :
			current->is_series_or_site() ? 3: 4;

		if (precedence > max)
		{
			const Ads_User_Experience_Config *ux_conf = nullptr;

			//FDB-6823 Scenario Forecasting - UEX
			Ads_Repository::Scenario_Section_UEC_Map::const_iterator lit = repo->uec_config_list_.find(current->id_);
			if (lit != repo->uec_config_list_.end())
			{
				const Ads_GUID_RVector &ux_list = lit->second;
				for (Ads_GUID_RVector::const_iterator it= ux_list.begin(); it != ux_list.end(); ++it)
				{
					Ads_Repository::User_Experience_Config_Map::const_iterator uit = repo->uec_config_.find(*it);
					if (uit == repo->uec_config_.end()) continue;
					if (forecast_scenario)
					{
						if (!forecast_scenario->allow_uec(*it, uit->second))
						{
							ADS_DEBUG((LP_DEBUG, "forecast scenario %ld not allow UEX %ld asset/section\n", forecast_scenario->id_, *it));
							continue;
						}
						ADS_DEBUG((LP_DEBUG, "forecast scenario %ld select UEX %ld asset/section\n", forecast_scenario->id_, *it));
						ux_conf = uit->second;
						break;
					}
					else if (!uit->second->is_scenario_)
					{
						ux_conf = uit->second;
						break;
					}
				}
			}

			if (ux_conf)
			{
				ret = ux_conf;
				max = precedence;
			}
		}

		pending.insert(pending.end(), current->parents_.begin(), current->parents_.end());
	}
	return ret;
}

const Ads_Asset_Base * Ads_Selector::downstream_p(const Ads_Asset_Base* asset,
		const Ads_Repository *repo, Ads_GUID network_id, const Ads_Selection_Context *ctx) const
{
	Ads_GUID_RVector::const_iterator end = asset->downstreams_.end();
	for (Ads_GUID_RVector::const_iterator it = asset->downstreams_.begin(); it != end; ++it)
	{
		Ads_GUID id = *it;
		const Ads_Asset_Base *entity = 0;
		if (ads::entity::type(id) == ADS_ENTITY_TYPE_ASSET)
		{
			if (ctx != nullptr)
			{
				ctx->find_asset(id, entity);
			}
			else
			{
				Ads_Repository::find_asset(repo, id, entity);
			}
		}
		else if (ads::entity::type(id) == ADS_ENTITY_TYPE_SECTION)
		{
			if (ctx != nullptr)
			{
				ctx->find_section(id, entity);
			}
			else
			{
				Ads_Repository::find_section(repo, id, entity);
			}
		}

		if (entity && entity->network_id_ == network_id)
			return entity;
	}

	return 0;
}

const Ads_Asset_Base::Meta_Data* Ads_Selector::metadata_set(const Ads_Asset_Base* asset,
		const Ads_Repository *repo, const Ads_RString& key, const Ads_Selection_Context* ctx) const
{
	const Ads_Asset_Base *ret = nullptr, *ros = nullptr, *ron = nullptr;

	Ads_GUID_Set processed;
	Ads_GUID_List pending;
	pending.push_back(asset->id_);

	while (!pending.empty())
	{
		Ads_GUID current_id = pending.front();
		pending.pop_front();

		if (processed.find(current_id) != processed.end()) continue;
		processed.insert(current_id);

		const Ads_Asset_Base *current = 0;
		if (current_id == asset->id_)
		{
			current = asset;
		}
		else if (ctx != nullptr && (ctx->find_asset_or_section(current_id, current) < 0 || !current))
		{
			continue;
		}
		else if (Ads_Repository::find_asset_or_section(repo, current_id, current) < 0 || !current)
		{
			continue;
		}

		// metadata
		if (current->meta_)
		{
			Ads_String_Multi_RMap::const_iterator it = current->meta_->extra_.find(key);
			if(it != current->meta_->extra_.end())
			{
				if (current->is_ros())
					ros = current;
				else if (current->is_run_of_network())
					ron = current;
				else if (current->is_group())
				{
					if (current->is_series_or_site())
					{
						ret = current;
						break;
					}

					if (!ret)
						ret = current;
				}
				else
				{
					ret = current;
					break;
				}
			}
		}

		pending.insert(pending.end(), current->parents_.begin(), current->parents_.end());
	}

	const auto *result = ret ? ret : ros ? ros : ron;
	if (result)
	{
		ADS_DEBUG((LP_DEBUG, "metadata key %s from asset %s\n", key.c_str(), ADS_ASSET_ID_TAG_CSTR(result->id_)));
	}
	return result ? result->meta_ : nullptr;
}

const char *Ads_Selector::meta(const Ads_Asset_Base* asset,
		const Ads_Repository *repo, const Ads_RString& key,
		const Ads_Selection_Context *ctx) const
{
	const auto* metadata = this->metadata_set(asset, repo, key, ctx);

	if (metadata)
	{
		const auto& it = metadata->extra_.find(key);
		if(it != metadata->extra_.end())
			return it->second.c_str();
	}

	return nullptr;
}

const Ads_Section_Base *Ads_Selector::website_root(const Ads_Section_Base* section,
		const Ads_Repository *repo, const Ads_Selection_Context *ctx) const
{
	const Ads_Section_Base * ret = 0;

	Ads_GUID_Set processed;
	Ads_GUID_List pending;
	pending.push_back(section->id_);

	while (!pending.empty())
	{
		Ads_GUID current_id = pending.front();
		pending.pop_front();

		if (processed.find(current_id) != processed.end()) continue;
		processed.insert(current_id);

		const Ads_Asset_Base *current = 0;
		if (current_id == section->id_)
		{
			current = section;
		}
		else if (ctx != nullptr && (ctx->find_asset_or_section(current_id, current) < 0 || !current))
		{
			continue;
		}
		else if (Ads_Repository::find_asset_or_section(repo, current_id, current) < 0 || !current)
		{
			continue;
		}

		// website root
		const Ads_Section_Base *section = reinterpret_cast<const Ads_Section_Base *>(current);

		// section is a website root if session_duration_ is set
		// section is a sitesection if session_duration is 0
		if (section->session_duration_ > 0)
		{
			if (section->is_ros()) continue;

			if (section->is_group())
			{
				if (section->is_series_or_site())
					return section;
				if (!ret) ret = section;
			}
			else
				return section;
		}

		pending.insert(pending.end(), current->parents_.begin(), current->parents_.end());
	}
	return ret;
}

const Ads_Section_Base::Refresh_Config * Ads_Selector::refresh_config(const Ads_Section_Base* section,
		const Ads_Repository *repo, const Ads_Selection_Context *ctx) const
{
	Ads_Section_Base::Refresh_Config::REPLACE_TYPE companion_replace_type = Ads_Section_Base::Refresh_Config::REPLACE_NOT_SET;
	Ads_Section_Base::Refresh_Config::REFRESH_TYPE display_refresh_type = Ads_Section_Base::Refresh_Config::REFRESH_NOT_SET;
	uint32_t display_refresh_interval = 0;

	Ads_GUID_Set processed;
	Ads_GUID_List pending;
	pending.push_back(section->id_);

	while (!pending.empty())
	{
		Ads_GUID current_id = pending.front();
		pending.pop_front();

		if (processed.find(current_id) != processed.end()) continue;
		processed.insert(current_id);

		const Ads_Asset_Base *current = 0;
		if (current_id == section->id_)
		{
			current = section;
		}
		else if (ctx != nullptr && (ctx->find_asset_or_section(current_id, current) < 0 || !current))
		{
			continue;
		}
		else if (Ads_Repository::find_asset_or_section(repo, current_id, current) < 0 || !current)
		{
			continue;
		}

		const Ads_Section_Base *section = reinterpret_cast<const Ads_Section_Base *>(current);

		if (companion_replace_type == Ads_Section_Base::Refresh_Config::REPLACE_NOT_SET
				&& section->companion_replace_type_ != Ads_Section_Base::Refresh_Config::REPLACE_NOT_SET)
			companion_replace_type = section->companion_replace_type_;
		if (display_refresh_type == Ads_Section_Base::Refresh_Config::REFRESH_NOT_SET
				&& section->display_refresh_type_ != Ads_Section_Base::Refresh_Config::REFRESH_NOT_SET) {
			display_refresh_type = section->display_refresh_type_;
			display_refresh_interval = section->display_refresh_interval_;
		}

		if (companion_replace_type != Ads_Section_Base::Refresh_Config::REPLACE_NOT_SET
				&& display_refresh_type != Ads_Section_Base::Refresh_Config::REFRESH_NOT_SET)
			break;

		pending.insert(pending.end(), current->parents_.begin(), current->parents_.end());
	}

	if (companion_replace_type != Ads_Section_Base::Refresh_Config::REPLACE_NOT_SET
			|| display_refresh_type != Ads_Section_Base::Refresh_Config::REFRESH_NOT_SET)
		return new Ads_Section_Base::Refresh_Config(companion_replace_type, display_refresh_type, display_refresh_interval);

	return 0;
}

bool Ads_Selector::is_compatible_with_standard_attributes(const Ads_MKPL_Listing::Split_Unit& su, const Ads_Selection_Context *ctx) const
{
	if (ads::entity::is_valid_id(su.brand_id_))
	{
		if (su.brand_id_ != ctx->standard_attributes_.brand_id_)
			return false;
	}

	if (ads::entity::is_valid_id(su.endpoint_owner_id_))
	{
		if (su.endpoint_owner_id_ != ctx->standard_attributes_.endpoint_owner_id_)
			return false;
	}

	return true;
}

int Ads_Selector::query_external_info(Ads_Selection_Context &ctx) const
{
	int res = 0;
	while ((res = ctx.external_info_state_->query(ctx)) == 0) {}

	return res;
}

#include "Ads_Selector.Closure.inl"
#include "Ads_Selector.Flash_Compatibility.inl"
#include "Ads_Selector.Response.inl"
#include "Ads_Selector.Log.inl"
#include "Ads_Selector.JSON.inl"
