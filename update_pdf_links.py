import os
import json

# 📁 Directory containing all your JSON chunks
json_dir = "/final_data/qdrant_chunks"

# 📌 Replace this with your actual PDF-to-link mapping
url_map = {
    # "factors_affecting_contract_farming_enforcement_in_rice_production_in_vietnam": "https://drive.google.com/file/d/new1",
    "12-steps-required-for-successful-rice-production": "https://drive.google.com/file/d/1D-k2TYikT60IhzbpAx3bhx-JYFA8gHws/view?usp=drive_link",
    "a_primer_on_organic-based_rice_farming_r.k._pandey": "https://drive.google.com/file/d/1josPwCX_-Rclhaxis8oLg1gHcDUlbQXt/view?usp=drive_link",
    "a_comparative_analysis_of_livelihood_sustainability_among_farmers_in_ca_mau_province__vietnam__a_case_study_of_organic_mangrove_shrimp_and_rice_shrimp": "https://drive.google.com/file/d/1tc-bs7SGmks3m_aA2ljJIQixNABCYaZl/view?usp=drive_link",
    "a-scoping-review-of-the-incentives-for-promoting-the-adoption-of-agroecological-practices-and-outcomes-among-rice-farmers-in-vietnam_2025_public-library-of-science": "https://drive.google.com/file/d/12e0jewQ92qhynWIw4uQuy80zrfJ8abvW/view?usp=drive_link",
    "assessment_of_impacts_of_adaptation_measures_on_rice_farm_economic_performance_in_response_to_climate_change_case_study_in_vietnam": "https://drive.google.com/file/d/1ylz2pz5g5fNFFW2DDvT0_LzbszYXB1sh/view?usp=drive_link",
    "benefits_of_mechanical_weeding_for_weed_control,_rice_growth_characteristics_and_yield_in_paddy_fields": "https://drive.google.com/file/d/1Dg534tdDU_g3jGqv4hcG-tkxAC7JOvmy/view?usp=drive_link",
    "climate_change_and_livelihood_vulnerability_of_the_rice_farmers_in_the_north_central_region_of_vietnam_a_case_study_in_nghe_an_province,_vietnam": "https://drive.google.com/file/d/1lEqDSmrUzmH-xK04CpSOzr9Bfmf_jMEP/view?usp=drive_link",
    "contract_farming_and_profitability_evidence_from_rice_crop_in_the_central_mekong_delta,_vietnam": "https://drive.google.com/file/d/1AQv0rwEUZZz0_r3vzK00v72eQxkqup72/view?usp=drive_link",
    "development_of_a_model_to_predict_the_throwing_trajectory_of_a_rice_seedling": "https://drive.google.com/file/d/1Kt0BwPUFhOYKCGb4oPQXxWT3SOoeg2Sw/view?usp=drive_link",
    "development_of_climate-related__risk_maps_and_adaptation_plans__(climate_smart_map)_for_rice_production_in_vietnam’s__mekong_river_delta":"https://drive.google.com/file/d/1oOTW1s_WXEVaBAQo4-xZTGjxpJbAInFE/view?usp=drive_link",
    "ecological_risk_assessment_of_pesticide_use_in_rice_farming_in_mekong_delta,_vietnam.": "https://drive.google.com/file/d/16y15W-8KRbcDQ5099xkYFGCXBGRcNYIz/view?usp=drive_link",
    "escaping_the_lock-in_to_pesticide_use_do_vietnamese_farmers_respond_to_flower_strips_as_a_restoration_practice_or_pest_management_action": "https://drive.google.com/file/d/1zE0ZcYBUh91bv4Nu1a_73K9cX-1Ed8r9/view?usp=drive_link",
    "factors_affecting_contract_farming_enforcement_in_rice_production_in_vietnam": "https://drive.google.com/file/d/13b1oyDIcY6sHI8-Jre03yKRnrcyiJMsA/view?usp=drive_link",
    "factorsaffectingorganicfertilizeradoptioninriceproductioninvietnam":"https://drive.google.com/file/d/16ttkddPVPgKkXupcrgsbkMZn87Rt9CRk/view?usp=drive_link",
    "farmers_in_the_midst_of_climate_change_an_intra-household_analysis_of_gender_roles_on_farmers’_choices_of_adaptation_strategies_to_salinity_intrusion_in_vietnam": "https://drive.google.com/file/d/1Uuk0VJS-4-cnSNMIdOBHUjbbW8dmfGqN/view?usp=drive_link",
    "growing_upland_rice_a_production": "https://drive.google.com/file/d/1iJaTP4litpxnrbLZc8z4DDgolSg3KBcl/view?usp=drive_link",
    "identifying_sustainable_rice_farming_strategies_in_the_mekong_delta_through_systems_analysis": "https://drive.google.com/file/d/1vZ6ExZfsgJMebnh24U75ZT0loRNgYo6b/view?usp=drive_link",
    "improved_manure_management_in_vietnam": "https://drive.google.com/file/d/1LUcpaUE0ZThwHX71bYyZOQuFzhlF1EiC/view?usp=drive_link",
    "investigation_of_current_cultivation_practices_and_weed_composition_in_rice_fields_in_hau_giang_province__vietnam": "https://drive.google.com/file/d/1a4PB0QDyrR2tBhCA-9RBs57bTnVLZLc_/view?usp=drive_link",
    "leaf_bleaching_in_rice-a_new_disease_in_vietnam": "https://drive.google.com/file/d/1g7rtOq68ySd33OGLgzDox8PmmqT7Gc-Z/view?usp=drive_link",
    "manure_management__in_vietnam": "https://drive.google.com/file/d/1PfxkzjxE7tjg_F0YSMj7rzBonCWSJ7J6/view?usp=drive_link",
    "organic_fertilizer_production_from_agriculture_by-products_for_sustainable_agricultural_systems": "https://drive.google.com/file/d/1V7f7eQLFvFd4_PR5dUUxTEsPzod1HZj7/view?usp=drive_link",
    "principles_and_practices_of_rice_production": "https://drive.google.com/file/d/1AdmFUUfDsbYUW2pEQfnyB_k9sOA3PArm/view?usp=drive_link",
    "recent_progress_in_rice_insect_research_in_vietnam": "https://drive.google.com/file/d/1suRmTaycTPRAYzOaqmNKv3Lc4cUK4cZJ/view?usp=drive_link",
    "rice_as_a_determinant_of_vietnamese_economic_susta": "https://drive.google.com/file/d/1s-S5pSdtwmh9OJ2Du5wYfKcgYqrtxtRf/view?usp=drive_link",
    "rice_breeding_in_vietnam_retrospects,_challenges_and_prospects": "https://drive.google.com/file/d/1ue7SGYJ8tyTuPe67Y7LLbpA9vkcSz9YK/view?usp=drive_link",
    "rice_soil_fertility_classification_in_the_mekong_d": "https://drive.google.com/file/d/1e7egyDeUEUcLKP5qvJPt9-QjPbFau6-B/view?usp=drive_link",
    "rice_variety_and_sustainable_farming":"https://drive.google.com/file/d/1nckcVLplB5Y-fhhs5zFhnqIY61rmfr5q/view?usp=drive_link",
    "soil_permeability_of_sandy_loam_and_clay_loam_soil_in_the_paddy_fields_in_an_giang_province_in_vietnam": "https://drive.google.com/file/d/1ChfATT2z3vyTSX4B-i7ySNyDF1Il_pYN/view?usp=drive_link",
    "system_of_rice_intensification_in_vietnam_doing_more_with_less": "https://drive.google.com/file/d/16me1Fk2UCEdR7EnoTIeThxDsJcimUjPK/view?usp=drive_link",
    "temperature_shocks,_rice_production,_and_migration_in_vietnamese_households": "https://drive.google.com/file/d/11YBra29TrIUtHeH5xAX9ugaBBEX4_DE3/view?usp=drive_link",
    "use_of_pesticides_and_attitude_to_pest_management_strategies_among_rice_and_rice-fish_farmers_inthemekong_delta__vietnam": "https://drive.google.com/file/d/1dXXECoLy5flao7AnVfMGOod0QPg-9CjP/view?usp=drive_link",
    "vietnam_and_irri,_a_partnership_in_rice_research_proceedings_of_a_conference_held_in_hanoi_vietnam": "https://drive.google.com/file/d/1_0StpYWNC7mutqP0kd9SUKNSIBXHzsxl/view?usp=drive_link",
    "vinh_long_province,_vietnam": "https://drive.google.com/file/d/1scitZP5JM4VxArpeN1-ePpVU5bKRTMqD/view?usp=drive_link",
    "weedy_rice_in_sustainable_rice_production._a_review": "https://drive.google.com/file/d/1wNPsrstbxY-O-psVKeWFA7mwyQitVnjG/view?usp=drive_link",
    "white_gold_the_commercialisation_of_rice_farming_in_the_lower_mekong_basin": "https://drive.google.com/file/d/1NljUZ_WQut13YlkcuIv9y7nqiNyyGs2c/view?usp=drive_link",
    "vietnam_rice,_farmers,_and_rural_development": "https://drive.google.com/file/d/1oQItnKATp86ihiNZm0bTt7x5zT1L4Y-N/view?usp=drive_link"

    # Add more mappings here...
}


updated_count = 0
skipped_count = 0

for filename in os.listdir(json_dir):
    if filename.endswith(".json"):
        path = os.path.join(json_dir, filename)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        payload = data.get("payload", {})
        source = payload.get("source", "")
        url = payload.get("url", "")

        if source == "pdf_import" and url.startswith("pdf://"):
            slug = url.replace("pdf://", "").strip()
            if slug in url_map:
                payload["url"] = url_map[slug]
                data["payload"] = payload

                with open(path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

                print(f"✅ Updated: {filename}")
                updated_count += 1
            else:
                print(f"⏭ Skipped (no mapping): {filename}")
                skipped_count += 1

print(f"\n🎯 Completed.")
print(f"✅ Updated files: {updated_count}")
print(f"⏭ Skipped (no match): {skipped_count}")
